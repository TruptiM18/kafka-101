package com.github.krishnatsm.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

/*
 Seek and Assign methods are used to avail replay facility to read messages
 from a particular topic.
*/
public class ConsumerDemoWithAssignAndSeek {
  public static void main(String[] args) {
    String topic = "secondTopic";
    // get logger
    Logger logger = LoggerFactory.getLogger(ConsumerDemoWithAssignAndSeek.class.getName());

    // set consumer properties
    Properties properties = new Properties();
    properties.setProperty(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ProjectConstants.BOOTSTRAP_SERVERS);
    properties.setProperty(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-seventh-application");
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // create consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

    // create topic partition
    TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);

    // set offset to read from
    long offsetToReadFrom = 15L;

    // assign topic to consumer
    consumer.assign(Collections.singletonList(partitionToReadFrom));

    // seek from offset=15 messages from topic secondTopic
    consumer.seek(partitionToReadFrom, offsetToReadFrom);

    int numberOfRecordsReadSoFar = 0;
    int numberOfRecordsToRead = 5;

    // poll for new data
    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord<String, String> record : records) {
        numberOfRecordsReadSoFar++;
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
        if (numberOfRecordsReadSoFar == numberOfRecordsToRead) {
          break;
        }
      }
      if (numberOfRecordsReadSoFar == numberOfRecordsToRead) {
        break;
      }
    }
  }
}
