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

import kafka.common.InvalidMessageSizeException;
import kafka.common.OffsetOutOfRangeException;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

/**
 * KafkaRiver
 *
 */
public class KafkaRiver extends AbstractRiverComponent implements River {

  private final Client client;
  private final KafkaRiverConfig riverConfig;

  private volatile boolean closed = false;
  private volatile Thread thread;

  @Inject
  public KafkaRiver(RiverName riverName, RiverSettings settings, Client client) {
    super(riverName, settings);
    this.client = client;

    try {
      logger.info("KafkaRiver created: name={}, type={}", riverName.getName(), riverName.getType());
      this.riverConfig = new KafkaRiverConfig(riverName.getName(), settings);
    } catch (Exception e) {
      logger.error("Unexpected Error occurred", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void start() {
    try {
      logger.info("creating kafka river: zookeeper = {}, name = {}, message_handler_factory_class = {}", riverConfig.zookeeper, riverConfig.riverName, riverConfig.factoryClass);
      logger.info("part = {}, topic = {}", riverConfig.partition, riverConfig.topic);
      logger.info("bulkSize = {}, bulkTimeout = {}", riverConfig.bulkSize, riverConfig.bulkTimeout);

      KafkaRiverWorker worker = new KafkaRiverWorker(this.createMessageHandler(client, riverConfig), riverConfig, client);

      thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "kafka_river").newThread(worker);
      thread.start();
    } catch (Exception e) {
      logger.error("Unexpected Error occurred", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    try {
      if (closed) {
        return;
      }
      logger.info("closing kafka river");
      closed = true;
      if (thread != null) {
        thread.interrupt();
      }
    } catch (Exception e) {
      logger.error("Unexpected Error occurred", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * createMessageHandler
   *
   *
   * @param client
   * @param config
   * @return
   * @throws Exception
   */
  private MessageHandler createMessageHandler(Client client, KafkaRiverConfig config) throws Exception{
    MessageHandlerFactory handlerfactory = null;
    try {
      handlerfactory = (MessageHandlerFactory) Class.forName(config.factoryClass).newInstance();
    } catch (Exception e) {
      logger.error("Unexpected Error occurred", e);
      throw new RuntimeException(e);
    }

    return handlerfactory.createMessageHandler(client);
  }

  /**
   * KafkaRiverWorker
   *
   *
   */
  private class KafkaRiverWorker implements Runnable {

    long offset;
    MessageHandler msgHandler;

    private KafkaClient kafka;
    private Client client;
    private KafkaRiverConfig riverConfig;

    StatsReporter statsd;
    private long statsLastPrintTime;
    private Stats stats = new Stats();

    public KafkaRiverWorker(MessageHandler msgHandler, KafkaRiverConfig riverConfig, Client client) throws Exception
    {
      this.msgHandler = msgHandler;
      this.client = client;
      this.riverConfig = riverConfig;
      reconnectToKafka();
      resetStats();
      initStatsd(riverConfig);
    }

    void initStatsd(KafkaRiverConfig riverConfig)
    {
      statsd = new StatsReporter(riverConfig);
      if(statsd.isEnabled())
      {
        logger.info("Created statsd client for prefix={}, host={}, port={}", riverConfig.statsdPrefix, riverConfig.statsdHost, riverConfig.statsdPort);
      }
      else
      {
        logger.info("Note: statsd is not configured, only console metrics will be provided");
      }
    }

    void resetStats()
    {
      statsLastPrintTime = System.currentTimeMillis();
      stats.reset();
    }

    void initKakfa()
    {
      this.kafka = new KafkaClient(riverConfig.zookeeper, riverConfig.topic, riverConfig.partition);
      this.offset = kafka.getOffset(riverConfig.riverName, riverConfig.topic, riverConfig.partition, riverConfig.startFromNewestOffset);
    }

    void handleMessages(BulkRequestBuilder bulkRequestBuilder, ByteBufferMessageSet msgs)
    {
      long numMsg = 0;
      for(MessageAndOffset mo : msgs)
      {
        ++numMsg;
        ++stats.numMessages;
        try {
          msgHandler.handle(bulkRequestBuilder, mo.message());
          offset = mo.nextOffset();
        } catch (Exception e) {
          logger.warn("Failed handling message", e);
        }
      }
      logger.debug("handleMessages processed {} messages", numMsg);
    }

    void executeBuilder(BulkRequestBuilder bulkRequestBuilder)
    {
      if(bulkRequestBuilder.numberOfActions() == 0)
        return;

      ++stats.flushes;
      BulkResponse response = bulkRequestBuilder.execute().actionGet();
      if (response.hasFailures()) {
        logger.warn("failed to execute" + response.buildFailureMessage());
      }

      for(BulkItemResponse resp : response){
        if(resp.isFailed()){
          stats.failed++;
        }else{
          stats.succeeded++;
        }
      }
    }

    void processNonEmptyMessageSet(ByteBufferMessageSet msgs)
    {
      logger.debug("Processing {} bytes of messages ...", msgs.validBytes());
      BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
      handleMessages(bulkRequestBuilder, msgs);
      executeBuilder(bulkRequestBuilder);
      kafka.saveOffset(riverConfig.riverName, riverConfig.topic, riverConfig.partition, offset);
    }

    void reconnectToKafka() throws InterruptedException
    {
      while(true)
      {
        if(closed)
          break;

        try {
          try {
            if (kafka != null) {
              kafka.close();
            }
          } catch (Exception e) {}

          initKakfa();
          break;
        }
        catch(Exception e2){
          logger.error("Error re-connecting to Kafka({}/{}), retrying in 5 sec", e2, riverConfig.topic, riverConfig.partition);
          Thread.sleep(5000);
        }
      }
    }

    long getBacklogSize()
    {
      return kafka.getNewestOffset(riverConfig.topic, riverConfig.partition) - offset;
    }

    void dumpStats()
    {
      long elapsed = System.currentTimeMillis() - statsLastPrintTime;
      if(elapsed >= 10000)
      {
        stats.backlog = getBacklogSize();
        stats.rate = (double)stats.numMessages/((double)elapsed/1000.0);
        logger.info("{}:{}/{}:{} {} msg ({} msg/s), flushed {} ({} err, {} succ) [msg backlog {}]",
            kafka.brokerHost, kafka.brokerPort, riverConfig.topic, riverConfig.partition,
            stats.numMessages, String.format("%.2f", stats.rate), stats.flushes,
            stats.failed, stats.succeeded,
            getBytesString(stats.backlog));

        statsd.reoportStats(stats);
        resetStats();
      }
    }

    @Override
    public void run() {

      try {
        logger.info("KafkaRiverWorker is running...");

        while(true)
        {
          if(closed)
            break;

          try {

            dumpStats();

            ByteBufferMessageSet msgs = kafka.fetch(riverConfig.topic, riverConfig.partition, offset, riverConfig.bulkSize);
            if(msgs.validBytes() > 0)
            {
              processNonEmptyMessageSet(msgs);
            }
            else
            {
              logger.debug("No messages received from Kafka for topic={}, partition={}, offset={}, bulkSize={}",
                  riverConfig.topic, riverConfig.partition, offset, riverConfig.bulkSize);
              Thread.sleep(1000);
            }
          }
          catch (InterruptedException e2) {
            break;
          }
          catch(OffsetOutOfRangeException e)
          {
            // Assumption: EITHER
            //
            //  1) This River is starting for the first time and Kafka has already aged some data out (so the lowest offset is not 0)
            //      OR
            //  2) This river has gotten far enough behind that Kafka has aged off enough data that the offset is no longer valid.
            //     If this is the case, this will likely happen everytime Kafka ages off old data unless the data flow decreases in volume.
            if (riverConfig.startFromNewestOffset) {
              logger.warn("Encountered OffsetOutOfRangeException, querying Kafka for newest Offset and reseting local offset");
              offset = kafka.getNewestOffset(riverConfig.topic, riverConfig.partition);
              logger.warn("Setting offset to oldest offset = {}", offset);
            }
            else {
              logger.warn("Encountered OffsetOutOfRangeException, querying Kafka for oldest Offset and reseting local offset");
              offset = kafka.getOldestOffset(riverConfig.topic, riverConfig.partition);
              logger.warn("Setting offset to oldest offset = {}", offset);
            }
          }
          catch (InvalidMessageSizeException e) {
            if (riverConfig.startFromNewestOffset) {
              logger.warn("InvalidMessageSizeException occurred for Kafka({}:{}/{}:{}), querying Kafka for newest Offset and reseting local offset", e, kafka.brokerHost, kafka.brokerPort, riverConfig.topic, riverConfig.partition);
              offset = kafka.getNewestOffset(riverConfig.topic, riverConfig.partition);
              logger.warn("Setting offset to oldest offset = {}", offset);
            }
            else {
              logger.warn("InvalidMessageSizeException occurred for Kafka({}:{}/{}:{}), querying Kafka for oldest Offset and reseting local offset", e, kafka.brokerHost, kafka.brokerPort, riverConfig.topic, riverConfig.partition);
              offset = kafka.getOldestOffset(riverConfig.topic, riverConfig.partition);
              logger.warn("Setting offset to oldest offset = {}", offset);
            }
            try {
              Thread.sleep(5000);
            } catch (InterruptedException e2) {
              break;
            }
          }
          catch (Exception e) {
            logger.error("Error fetching from Kafka({}:{}/{}:{}), retrying in 5 sec", e, kafka.brokerHost, kafka.brokerPort, riverConfig.topic, riverConfig.partition);
            try {
              Thread.sleep(5000);
              reconnectToKafka();
            } catch (InterruptedException e2) {
              break;
            }
          }
        } // end while
        kafka.close();
        logger.info("KafkaRiverWorker is stopping...");
      } catch (Exception e) {
        logger.error("Unexpected Error Occurred", e);

        // Don't normally like to rethrow exceptions like this, but ES silently ignores them in Plugins
        throw new RuntimeException(e);
      }
    } // end run
  }

  /**
   * @param bytes
   * @return
   */
  static String getBytesString(long bytes)
  {
    String size;
    if( Math.floor(bytes/(1024*1024*1024)) > 0){
      size = String.format("%.2f GB", (double)bytes/(1024.0*1024.0*1024.0));
    }
    else if( Math.floor(bytes/(1024*1024)) > 0){
      size = String.format("%.2f MB", (double)bytes/(1024.0*1024.0));
    }
    else if( Math.floor(bytes/(1024)) > 0){
      size = String.format("%.2f KB", (double)bytes/(1024.0));
    }
    else{
      size = bytes+" B";
    }
    return size;
  }
}
