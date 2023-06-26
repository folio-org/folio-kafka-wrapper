## 2023-06-26 v2.7.2
* [KAFKAWRAP-37](https://issues.folio.org/browse/KAFKAWRAP-37) Implement fetch method

## 2023-03-06 v2.7.1
* [KAFKAWRAP-34](https://issues.folio.org/browse/KAFKAWRAP-34) Reduce default number of partitions from 50 to 1

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
