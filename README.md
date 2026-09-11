# folio-kafka-wrapper

Copyright (C) 2021–2026 The Open Library Foundation

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

Creating consumer wrapper over static consumer (when [group.instance.id](https://kafka.apache.org/documentation/#consumerconfigs_group.instance.id) is set)  
Declaring static consumer using builder method:
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
        .groupInstanceId(groupInstanceId)
        .build();
```
Declaring static consumer using setter:
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
consumerWrapper.setGroupInstanceId(groupInstanceId);
```
## Kafka Tenant Filtering

Kafka tenant filtering allows `KafkaConsumerWrapper` to skip Kafka messages for tenants for which the module is not 
enabled. Filtering is disabled by default. Modules can enable it by setting `FOLIO_KAFKA_TENANT_FILTER_ENABLED=true`.

Filtering needs the module's moduleId in `<artifactId>-<version>` format (for example
`mod-foo-1.2.3`). Pass the moduleId as the third argument to `start(handler, consumerGroupSuffix, moduleId)` method:
```java
consumerWrapper.start(getHandler(), "mod-foo-1-" + UUID.randomUUID(), "mod-foo-1.2.3");
```
The existing two-argument `start(handler, consumerGroupSuffix)` is deprecated: it passes no
`moduleId`, so `start()` fails fast if filtering is enabled - use the three-argument form instead.

This is the Vert.x/RMB counterpart to `folio-spring-kafka`'s tenant-aware `RecordFilterStrategy`,
described in [folio-spring-support's README](https://github.com/folio-org/folio-spring-support#kafka-tenant-filtering) -
the two libraries share the same environment variable names for the settings they have in common.

### How filtering works

When enabled, for each record:

1. Reads the `x-okapi-tenant` value from Kafka record headers. The filter does not deserialize the message body.
2. Checks if the tenant is entitled to the module, using the in-process entitlement cache described below.
3. If the tenant is entitled, hands the record to the business handler.
4. Applies `TENANT_DISABLED_STRATEGY` when the tenant is not entitled to the current module.
5. Applies `ALL_TENANTS_DISABLED_STRATEGY` when no tenants are entitled to the current module.

### How the entitlement cache stays up to date

The entitled-tenants set is cached in-process and kept current three ways:
1. On the first Kafka record seen with an unpopulated cache, an async fetch from the sidecar
   (`GET /entitlements/modules/{moduleId}`) starts, retrying indefinitely with a capped backoff on
   failure. Kafka records seen while that fetch is still in flight wait up to 10 seconds for it to
   finish; if it still hasn't finished by then, `ALL_TENANTS_DISABLED_STRATEGY` is applied, the same
   as when the cache loads but comes back empty.
2. Direct updates from `ENTITLE`/`UPGRADE`/`REVOKE` events on the `entitlement` Kafka topic. Each
   module instance uses its own unique consumer group id, so every instance observes every event.
3. A periodic full re-fetch that corrects any drift from a missed or duplicate event.

### Configuration

| Environment variable                                               | Description                                                                        | Default | Example  |
|--------------------------------------------------------------------|------------------------------------------------------------------------------------|---------|----------|
| `FOLIO_KAFKA_TENANT_FILTER_ENABLED`                                | Enables entitlement-based filtering.                                               | `false` | `true`   |
| `FOLIO_KAFKA_TENANT_FILTER_TENANT_DISABLED_STRATEGY`               | Strategy used when the message tenant is not entitled to the current module.       | `SKIP`  | `SKIP`   |
| `FOLIO_KAFKA_TENANT_FILTER_ALL_TENANTS_DISABLED_STRATEGY`          | Strategy used when no tenants are entitled to the current module.                  | `FAIL`  | `SKIP`   |
| `FOLIO_KAFKA_TENANT_FILTER_ENTITLEMENT_REFRESH_INTERVAL_SECONDS`   | How often, in seconds, the entitlement cache is fully re-fetched from the sidecar. | `900`   | `300`    |

The following strategy values are supported:

| Value    | Behavior                                                                                                                             |
|----------|----------------------------------------------------------------------------------------------------------------------------------------|
| `ACCEPT` | Accept the record and hand it to the business handler for normal processing.                                                        |
| `SKIP`   | Skip the record without invoking the business handler. The offset still commits, same as a normally-processed record.               |
| `FAIL`   | Route the record to the configured `processRecordErrorHandler` (if any), the same way a business handler failure is; the offset still commits - there is no automatic retry. |

## Environment Variables
* **KAFKA_PRODUCER_TENANT_COLLECTION**: Set to a value matching [A-Z][A-Z0-9]{0,30} .
This will enable messages to be produced to a tenant collection topic with the "tenantId"
set to the value of this environment variable.

## Additional information

* See project [KAFKAWRAP](https://issues.folio.org/browse/KAFKAWRAP)
at the [FOLIO issue tracker](https://dev.folio.org/guidelines/issue-tracker).

* Other FOLIO Developer documentation is at [dev.folio.org](https://dev.folio.org/)
