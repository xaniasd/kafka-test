# Kafka test

## Overview
This repository contains several Kafka-related subprojects for learning purposes.

## Sub-projects

### AvroProducer/AvroConsumer

TestProducer is a simple producer that publishes 100 events to a Kafka topic (```CustomerContacts```) using an Avro schema.
TestConsumer consumes these events and prints them to standard output.

Both use the confluent Schema Registry, assumed available at ```schema-registry:8081```