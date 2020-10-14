package com.github.krishnatsm.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/*
Note on this code-
* ConsumerRunnable:
run() - runs a while(true) loop to continuously consume the kafka records.
shutdown() - calls consumer.wakeup() - special method to interrupts consumer.poll()

* Main method-
1. creates a latch
2. creates a shutdown hook to handle graceful application exits.
3. creates a runnable of consumerRunnable and gives it to a consumerThread. Starts the thread execution.
4. await till the latch_count=0

* Whenever user wants to stop the consumer, he/she will try to exit the application
1. In that case control will go to shutdown hook.
2. Shutdown hook spins a new thread which calls shutdown method of the Runnable which in turns call consumer.wakeup()
3. Gotcha- This newly spin thread of shutdown hook also needs to call latch.await() to make sure that shutdown is
called, WakeupException is caught and consumer is closed.
4. For that particular instance just after shutdown method of consumerRunnable is called inside shutdown hook,
there will be 2 threads waiting for the latch.await() - one is main() and other is runnable thread passed as
argument to shutdown hook.
Once control comes out of while(true) loop, consumerThread execution is complete and two threads will proceed with their
execution.

*/
public class ConsumerGroupsDemo {
  public static void main(String[] args) {
    ConsumerGroupsDemo consumerGroupsDemo = new ConsumerGroupsDemo();
    consumerGroupsDemo.run();
  }

  public ConsumerGroupsDemo() {}

  public void run() {
    String topic = "secondTopic";
    String groupId = "my-sixth-application";

    // get logger
    Logger logger = LoggerFactory.getLogger(ConsumerGroupsDemo.class.getName());
    // get the latch for dealing with multiple threads
    CountDownLatch latch = new CountDownLatch(1);
    // get runnable
    Runnable consumerRunnable = new ConsumerRunnable(latch, topic, groupId);
    // create consumer runnable
    Thread consumerThread = new Thread(consumerRunnable);
    // start the thread
    consumerThread.start();
    // add shutdown hook - executes this code whenever an application is exited gracefully
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  logger.info("Caught shutdown hook");
                  ((ConsumerRunnable) consumerRunnable).shutdown();
                  // make the application wait till all other thread call countDown() method
                  try {
                    //wait until consumerThread is done with its execution
                    latch.await();
                  } catch (InterruptedException e) {
                    e.printStackTrace();
                  } finally {
                    logger.info("Application has exited");
                  }
                }));
    // make the application wait till all other thread call countDown() method
    try {
      //wait until consumerThread is done with its execution
      latch.await();
    } catch (InterruptedException e) {
      logger.error("Application got interrupted ", e);
      e.printStackTrace();
    } finally {
      logger.info("Application is closing");
    }
  }

  public class ConsumerRunnable implements Runnable {
    private CountDownLatch latch;
    private Logger logger;
    private KafkaConsumer<String, String> consumer;
    private String topic;
    private String groupId;

    public ConsumerRunnable(CountDownLatch latch, String topic, String groupId) {
      // Get logger
      logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
      this.latch = latch;
      this.topic = topic;
      this.groupId = groupId;
      // set consumer properties
      Properties properties=getProperties();
      this.consumer = new KafkaConsumer<>(properties);
    }

    @Override
    public void run() {
      // subscribe consumer to topic(s)
      consumer.subscribe(Collections.singletonList(topic));
      try {
        // poll for new data
        while (true) {
          ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
          for (ConsumerRecord<String, String> record : records) {
            logger.info(
                "Key: "
                    + record.key()
                    + " Value: "
                    + record.value()
                    + "\n Topic: "
                    + record.topic()
                    + "\n Partition: "
                    + record.partition()
                    + "\n Offset: "
                    + record.offset());
          }
        }
      } catch (WakeupException wakeupException) {
        logger.info("Received a shutdown signal");
      } finally {
        consumer.close();
        latch.countDown();
      }
    }

    public void shutdown() {
      // wakeup() is special method to interrupts consumer.poll(). It will throw exception
      // wakeUpException
      consumer.wakeup();
    }
    public Properties getProperties(){
      Properties properties = new Properties();
      properties.setProperty(
              ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ProjectConstants.BOOTSTRAP_SERVERS);
      properties.setProperty(
              ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      properties.setProperty(
              ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
      properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      return properties;
    }
  }
}
