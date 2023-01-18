package org.folio.kafka.services;

import org.folio.kafka.KafkaConfig;

import static java.lang.String.join;
import static org.folio.kafka.KafkaTopicNameHelper.formatTopicName;
import static org.folio.kafka.services.KafkaEnvironmentProperties.environment;

public interface KafkaTopic {

  /**
   * Returns module name
   */
  String moduleName();

  /**
   * Returns topic name
   */
  String topicName();

  /**
   * Returns num partitions
   * Default - 50
   */
  default int numPartitions() {
    return 50;
  }

  /**
   * Returns replication factor.
   * Default - environment variable 'REPLICATION_FACTOR'
   */
  default short replicationFactor(){
    return KafkaEnvironmentProperties.replicationFactor();
  }

  /**
   * Returns module topic name.
   * Order: {modulePrefix}.{topicName}
   */
  default String moduleTopicName() {
    return join(".", moduleName(), topicName());
  }

  /**
   * Returns full topic name. Based on environment variables and system properties
   * Order: {environment}.{tenantId}.{modulePrefix}.{topicName}
   */
  default String fullTopicName(String tenant) {
    return formatTopicName(environment(), tenant, moduleName(), topicName());
  }

  /**
   * Returns full topic name. Based on kafka configuration
   * Order: {environment}.{tenantId}.{modulePrefix}.{topicName}
   */
  default String fullTopicName(KafkaConfig config, String tenant) {
    return formatTopicName(config.getEnvId(), tenant, moduleName(), topicName());
  }
}
