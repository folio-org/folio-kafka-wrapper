## 2024-10-28 v3.2.0
* [KAFKAWRAP-54](https://folio-org.atlassian.net/browse/KAFKAWRAP-54) Do not delete kafka topics if collection topic is enabled
* [KAFKAWRAP-58](https://folio-org.atlassian.net/browse/KAFKAWRAP-58) Add ability to configure Kafka topic

## 2024-03-18 v3.1.0
* [KAFKAWRAP-50](https://issues.folio.org/browse/KAFKAWRAP-50) KafkaConsumerWrapper.fetch Should Set Consumer to Resumed State
* [KAFKAWRAP-52](https://folio-org.atlassian.net/browse/KAFKAWRAP-52) Allow Consumer Deserializers to be Configured

## 2023-10-11 v3.0.0
* [KAFKAWRAP-39](https://issues.folio.org/browse/KAFKAWRAP-39) Upgrade folio-kafka-wrapper to Java 17
* [KAFKAWRAP-34](https://issues.folio.org/browse/KAFKAWRAP-34) Reduce default number of partitions from 50 to 1
* [KAFKAWRAP-37](https://issues.folio.org/browse/KAFKAWRAP-37) Added fetch() method to KafkaConsumerWrapper
* [KAFKAWRAP-38](https://issues.folio.org/browse/KAFKAWRAP-38) Add Tenant Id To Every Produced Message
* [KAFKAWRAP-41](https://issues.folio.org/browse/KAFKAWRAP-41) Add tenant collection topics
* [KAFKAWRAP-45](https://issues.folio.org/browse/KAFKAWRAP-45) Propagate Logging Context From Kafka Headers
* [KAFKAWRAP-46](https://issues.folio.org/browse/KAFKAWRAP-46) Consider global load sensor when threshold exceeded
* [MODINVSTOR-1076](https://issues.folio.org/browse/MODINVSTOR-1076) Add X-Okapi-Token header to kafka producer record headers
* [KAFKAWRAP-47](https://issues.folio.org/browse/KAFKAWRAP-47) Extend KafkaTopicNameHelper to have formatTopicName without nameSpace

## 2023-03-02 v2.7.0
* [MODDATAIMP-750](https://issues.folio.org/browse/MODDATAIMP-750) Update util dependencies
* [MODDATAIMP-736](https://issues.folio.org/browse/MODDATAIMP-736) Adjust logging configuration to display datetime in a proper format
* [KAFKAWRAP-21](https://issues.folio.org/browse/KAFKAWRAP-21) Logging improvement
* [KAFKAWRAP-30](https://issues.folio.org/browse/KAFKAWRAP-30) Logging improvement - Configuration
* [KAFKAWRAP-29](https://issues.folio.org/browse/KAFKAWRAP-29) Implement mechanism of topic creation
* [KAFKAWRAP-28](https://issues.folio.org/browse/KAFKAWRAP-28) Fixed sporadic failures in KafkaConsumerWrapperTest

## 2022-10-18 v2.6.1
* [KAFKAWRAP-25](https://issues.folio.org/browse/KAFKAWRAP-25) Upgrade dependencies fixing vulnerabilities
* [KAFKAWRAP-26](https://issues.folio.org/browse/KAFKAWRAP-26) Publish jars with source code and javadoc

## 2022-05-23 v2.6.0
* [KAFKAWRAP-18](https://issues.folio.org/browse/KAFKAWRAP-18) Add implementation pause/resume methods to support DI flow control
* [KAFKAWRAP-22](https://issues.folio.org/browse/KAFKAWRAP-22) Upgrade vulns: log4j-core, jackson-databind, netty-codec-http

## 2022-02-22 v2.5.0
* [KAFKAWRAP-3](https://issues.folio.org/browse/KAFKAWRAP-3) Implement error handler contract for KafkaConsumerWrapper
* [MODDATAIMP-623](https://issues.folio.org/browse/MODDATAIMP-623) Remove Kafka cache initialization and Maven dependency
* [KAFKAWRAP-4](https://issues.folio.org/browse/KAFKAWRAP-4) Cover with tests folio-kafka-wrapper

## 2021-09-29 v2.4.0
* [KAFKAWRAP-2](https://issues.folio.org/browse/KAFKAWRAP-2) Take folio-kafka-wrapper lib out of mod-pubsub repository
* [KAFKAWRAP-10](https://issues.folio.org/browse/KAFKAWRAP-10) Provide property to set compression type for producer configuration
* [KAFKAWRAP-11](https://issues.folio.org/browse/KAFKAWRAP-11) Remove duplicate kafka headers
* [MODDATAIMP-557](https://issues.folio.org/browse/MODDATAIMP-557) Close Vertx Kafka producers that have not been closed
* [MODDATAIMP-623](https://issues.folio.org/browse/MODDATAIMP-623) Remove Kafka cache initialization and Maven dependency
