package com.github.krishnatsm.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallbackDemo {
  public static void main(String[] args) {
    // get logger
    final Logger logger = LoggerFactory.getLogger(ProducerWithCallbackDemo.class);
    // create producer properties
    Properties properties = new Properties();
    properties.setProperty(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ProjectConstants.BOOTSTRAP_SERVERS);
    properties.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // create the producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
    String topic="secondTopic";
    String value="hello world";
    for (int i = 0; i < 10; i++) {
      // create producer record
      ProducerRecord<String, String> record =
          new ProducerRecord<String, String>(topic,value);

      // send the data - asynchronous
      producer.send(
          record,
          new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
              // executes every time when a record is successfully sent or an exception is thrown
              if (e == null) {
                // record was sent successfully
                logger.info(
                    "New metadata received.\n"
                        + "Topic: "
                        + recordMetadata.topic()
                        + "\nPartition: "
                        + recordMetadata.partition()
                        + "\nOffset: "
                        + recordMetadata.offset()
                        + "\nTimestamp: "
                        + recordMetadata.timestamp());
              } else {
                // exception occurred
                logger.error("Error while producing: " + e);
              }
            }
          });
    }

    // flush and close the producer
    producer.flush();
    producer.close();
  }
}
