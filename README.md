# folio-kafka-wrapper

Copyright (C) 2021 The Open Library Foundation

This software is distributed under the terms of the Apache License,
Version 2.0. See the file "[LICENSE](LICENSE)" for more information.

## Introduction

Utilities for data import modules interaction with Kafka

## Usage
Creating A Configuration
```java
KafkaConfig kafkaConfig = KafkaConfig.builder()
      .envId(envId)
      .kafkaHost(kafkaHost)
      .kafkaPort(kafkaPort)
      .okapiUrl(okapiUrl)
      .replicationFactor(replicationFactor)
      .maxRequestSize(maxRequestSize)
      .build();
```
Creating A Topic
```java
KafkaAdminClientService kafkaAdminClientService = new KafkaAdminClientService(vertx);
kafkaAdminClientService.createKafkaTopics(DataImportKafkaTopic.values(), tenantId);
```
Creating A Producer
```java
var producerManager = new SimpleKafkaProducerManager(vertxContext.owner(), kafkaConfig);
var producer = producerManager.createShared(kafkaTopic);
```
Creating A Record
```java
var record = new KafkaProducerRecordBuilder<String, Object>("tenantId")
      .key(key)
      .value(value)
      .topic(kafkaTopic)
      .propagateOkapiHeaders(okapiHeaders)
      .build();
```
Producing a Record
```java
producer.send(record)
        .onFailure(error -> {
          log.error("Unable to send event [{}]", producerRecord.value(), error);
          failureHandler.handleFailure(error, producerRecord);
        });
```
Consuming a Record
```java
KafkaConsumerWrapper<String, String> consumerWrapper = KafkaConsumerWrapper.<String, String>builder()
        .context(context)
        .vertx(vertx)
        .kafkaConfig(kafkaConfig)
        .loadLimit(loadLimit)
        .globalLoadSensor(globalLoadSensor)
        .subscriptionDefinition(subscriptionDefinition)
        .processRecordErrorHandler(getErrorHandler())
        .backPressureGauge(getBackPressureGauge())
        .build();
consumerWrapper.start(getHandler(), "mod-business-logic-1.2.4");
```
## Environment Variables
* **KAFKA_PRODUCER_TENANT_COLLECTION**: Set to a value matching [A-Z][A-Z0-9]{0,30} .
This will enable messages to be produced to a tenant collection topic with the "tenantId"
set to the value of this environment variable.

## Additional information

* See project [KAFKAWRAP](https://issues.folio.org/browse/KAFKAWRAP)
at the [FOLIO issue tracker](https://dev.folio.org/guidelines/issue-tracker).

* Other FOLIO Developer documentation is at [dev.folio.org](https://dev.folio.org/)
