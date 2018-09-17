package org.example.kafka;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AvroProducer {
  private static Logger LOGGER = LoggerFactory.getLogger(AvroProducer.class);

  public static void main(String[] args) throws InterruptedException, IOException {

    KafkaProducer<String, GenericRecord> producer;
    try (InputStream props = Resources.getResource("producer.properties").openStream()) {
      Properties kafkaProps = new Properties();
      kafkaProps.load(props);
      producer = new KafkaProducer<>(kafkaProps);
    }

    String schemaString;

    try {
      schemaString = Resources.toString(Resources.getResource("customer.avsc"), Charsets.UTF_8);
    } catch (IOException exception) {
      exception.printStackTrace();
      throw exception;
    }

    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(schemaString);

    for (int i = 0; i < 100; i++) {
      GenericRecord customer = new GenericData.Record(schema);
      customer.put("id", i);
      String name = "customer-" + i;
      customer.put("name", name);
      ProducerRecord<String, GenericRecord> record = new ProducerRecord<>("CustomerContacts", name, customer);
      LOGGER.info("Sending customer " + name);
      producer.send(record, (metadata, exception) -> {
        if (exception != null) {
          exception.printStackTrace();
        }
      });
    }

    producer.flush();
    producer.close();

    Thread.sleep(1000);
  }
}
