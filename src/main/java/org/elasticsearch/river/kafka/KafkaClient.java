/* Copyright 2013 Endgame, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.river.kafka;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.*;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;

import org.apache.zookeeper.CreateMode;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.type.TypeReference;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaClient {
  ESLogger logger;
  CuratorFramework curator;
  SimpleConsumer consumer;
  String clientName;
  String brokerHost;
  int brokerPort;
  final ObjectReader jsonReader = new ObjectMapper().reader(new TypeReference<Map<String, Object>>() {});

  public KafkaClient(String zk, String topic, int partition)
  {
    logger = ESLoggerFactory.getLogger("kafka-client");
    clientName ="Client_" + topic + "_" + partition;
    connect(zk, topic, partition);
  }

  void connect(String zk, String topic, int partition)
  {
    try {
      // Start ZK curator
      curator = CuratorFrameworkFactory.newClient(zk, 1000, 15000, new RetryNTimes(5, 2000));
      curator.start();
      
      // Get all brokers from ZK
      List<Map<String, Object>> brokers = getBrokers();
      // Discover the lead broker for the topic/partition from the list of brokers
      PartitionMetadata metadata = findLeader(brokers, topic, partition);

      brokerHost = metadata.leader().host();
      brokerPort = metadata.leader().port();
      logger.info(String.format("Leader for %s/%d is broker %s:%d", topic, partition, brokerHost, brokerPort));

      // Create SimpleConsumer for lead broker
      consumer = new SimpleConsumer(brokerHost, brokerPort, 1000, 1024*1024*10, clientName);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private List<Map<String, Object>> getBrokers() throws Exception {
    List<String> brokerIds = getChildren("/brokers/ids");
    List<Map<String, Object>> brokers = new ArrayList<Map<String, Object>>();

    for (String brokerId : brokerIds) {
      String brokerInfo = get(String.format("/brokers/ids/%s", brokerId));
      logger.info(String.format("Discovered broker %s %s", brokerId, brokerInfo));
      Map<String, Object> brokerMap = jsonReader.readValue(brokerInfo);
      brokers.add(brokerMap);
    }
    return brokers;
  }

  private PartitionMetadata findLeader(List<Map<String, Object>> seedBrokers, String topic, int partition) {
    PartitionMetadata metadata = null;
    loop:
    for (Map<String, Object> seed : seedBrokers) {
      String seedBrokerHost = (String)seed.get("host");
      int seedBrokerPort = (int)seed.get("port");
      SimpleConsumer seedConsumer = null;
      try {
          seedConsumer = new SimpleConsumer(seedBrokerHost, seedBrokerPort, 1000, 1024*1024*10, "leaderLookup");

          List<String> topics = Collections.singletonList(topic);
          TopicMetadataRequest req = new TopicMetadataRequest(topics);
          TopicMetadataResponse resp = seedConsumer.send(req);

          List<TopicMetadata> metaData = resp.topicsMetadata();
          for (TopicMetadata item : metaData) {
            for (PartitionMetadata part : item.partitionsMetadata()) {
              if (part.partitionId() == partition) {
                  metadata = part;
                  break loop;
              }
            }
          }
        } catch (Exception e) {
            logger.warn("Error communicating with Broker [" + seed + "] to find Leader for [" + topic
                    + ", " + partition + "] Reason: " + e);
        } finally {
            if (seedConsumer != null) {
              seedConsumer.close();
            }
        }
    }
    if (metadata == null) {
      throw new RuntimeException("Can't find metadata for Topic and Partition. Exiting");
    }
    if (metadata.leader() == null) {
      throw new RuntimeException("Can't find Leader for Topic and Partition. Exiting");
    }
    return metadata;
  }

  public void save(String path, String data)
  {
    try {
      if(curator.checkExists().forPath(path) == null){
        curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path, data.getBytes());
      }
      else{
        curator.setData().forPath(path, data.getBytes());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public String get(String path) {
    try {
      if (curator.checkExists().forPath(path) != null) {
        return new String(curator.getData().forPath(path));
      } else {
        return null;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public List<String> getChildren(String path) {
    try {
      if (curator.checkExists().forPath(path) != null) {
        return curator.getChildren().forPath(path);
      } else {
        return null;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void saveOffset(String riverName, String topic, int partition, long offset)
  {
    save(String.format("/es-river-kafka/%s/offsets/%s/%d", riverName, topic, partition), Long.toString(offset));
  }

  public long getOffset(String riverName, String topic, int partition, boolean startFromNewestOffset) {
    String data = get(String.format("/es-river-kafka/%s/offsets/%s/%d", riverName, topic, partition));
    if(data == null) {
      if (startFromNewestOffset) {
        return getNewestOffset(topic, partition);
      }
      else {
        return getOldestOffset(topic, partition);
      }
    }
    return Long.parseLong(data);
  }

  public long getNewestOffset(String topic, int partition) {
    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
    requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 1));
    OffsetRequest request = new OffsetRequest(
        requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
    OffsetResponse response = consumer.getOffsetsBefore(request);
    return response.offsets(topic, partition)[0];
  }

  public long getOldestOffset(String topic, int partition) {
    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
    requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.EarliestTime(), 1));
    OffsetRequest request = new OffsetRequest(
        requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
    OffsetResponse response = consumer.getOffsetsBefore(request);
    return response.offsets(topic, partition)[0];
  }

  ByteBufferMessageSet fetch(String topic, int partition, long offset, int maxSizeBytes)
  {
    FetchRequest req = new FetchRequestBuilder()
        .clientId(clientName)
        .addFetch(topic, partition, offset, maxSizeBytes)
        .build();
    FetchResponse fetchResponse = consumer.fetch(req);
    if (fetchResponse.hasError()) {
      short code = fetchResponse.errorCode(topic, partition);
      ErrorMapping.maybeThrowException(code);
    }
    return fetchResponse.messageSet(topic, partition);
  }

  public void close() {
    curator.close();
  }
}
