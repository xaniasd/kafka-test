package org.example.kafka;

import com.google.common.io.Resources;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


public class AvroConsumer {
  private static Logger LOGGER = LoggerFactory.getLogger(AvroConsumer.class);

  public static void main(String[] args) throws IOException {
    KafkaConsumer<String, GenericRecord> consumer;

    try (InputStream props = Resources.getResource("consumer.properties").openStream()) {
      Properties kafkaProps = new Properties();
      kafkaProps.load(props);
      consumer = new KafkaConsumer<>(kafkaProps);
    }

    consumer.subscribe(Collections.singletonList("CustomerContacts"));
    Duration timeout = Duration.ofMillis(100);
    try {
      while (true) {
        ConsumerRecords<String, GenericRecord> records = consumer.poll(timeout);
        for (ConsumerRecord<String, GenericRecord> record : records) {
          LOGGER.info(
            String.format("topic=%s, partition=%d, offset=%d, key=%s, value=%s\n",
              record.topic(),
              record.partition(),
              record.offset(),
              record.key(),
              record.value()
            )
          );
        }
        consumer.commitSync();
      }
    } catch (CommitFailedException exception) {
      LOGGER.warn("Commit failed", exception);
    } finally {
      consumer.close();
    }
  }
}
