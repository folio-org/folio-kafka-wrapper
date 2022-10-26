package org.folio.kafka.services;

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
   * Returns full topic name.
   * Order: {environment}.{tenantId}.{modulePrefix}.{topicName}
   */
  default String fullTopicName(String tenant) {
    return formatTopicName(environment(), tenant, moduleName(), topicName());
  }
}
