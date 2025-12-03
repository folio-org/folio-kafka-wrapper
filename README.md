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
## Environment Variables
* **KAFKA_PRODUCER_TENANT_COLLECTION**: Set to a value matching [A-Z][A-Z0-9]{0,30} .
This will enable messages to be produced to a tenant collection topic with the "tenantId"
set to the value of this environment variable.

## Security Configuration

Security properties contain dots in their names, which are not supported as environment variables in most shells. Use Java system properties via `JAVA_OPTIONS`:

```bash
JAVA_OPTIONS="-Dsecurity.protocol=SASL_SSL -Dssl.truststore.location=/path/to/truststore.jks"
```

### SSL Properties
| Property | Description |
|----------|-------------|
| `security.protocol` | Protocol: `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` (default: `PLAINTEXT`) |
| `ssl.protocol` | SSL/TLS protocol version (default: `TLSv1.2`) |
| `ssl.truststore.location` | Path to truststore file |
| `ssl.truststore.password` | Truststore password |
| `ssl.truststore.type` | Truststore type (default: `JKS`) |
| `ssl.keystore.location` | Path to keystore file |
| `ssl.keystore.password` | Keystore password |
| `ssl.keystore.type` | Keystore type (default: `JKS`) |
| `ssl.key.password` | Private key password |
| `ssl.endpoint.identification.algorithm` | Hostname verification algorithm (set to empty string to disable) |

### SASL Properties
| Property | Description |
|----------|-------------|
| `sasl.mechanism` | SASL mechanism: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER` |
| `sasl.jaas.config` | JAAS configuration string |
| `sasl.oauthbearer.token.endpoint.url` | OAuth token endpoint URL (for OAUTHBEARER) |
| `sasl.login.callback.handler.class` | Login callback handler class (for OAUTHBEARER) |
| `sasl.login.connect.timeout.ms` | Connection timeout for login in milliseconds (for OAUTHBEARER) |
| `sasl.login.read.timeout.ms` | Read timeout for login in milliseconds (for OAUTHBEARER) |

Spring-style property names (e.g., `spring.kafka.security.protocol`) are also supported.

## Additional information

* See project [KAFKAWRAP](https://issues.folio.org/browse/KAFKAWRAP)
at the [FOLIO issue tracker](https://dev.folio.org/guidelines/issue-tracker).

* Other FOLIO Developer documentation is at [dev.folio.org](https://dev.folio.org/)
