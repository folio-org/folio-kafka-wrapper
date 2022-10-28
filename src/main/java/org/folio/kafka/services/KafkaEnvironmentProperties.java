package org.folio.kafka.services;

import static java.lang.Short.parseShort;
import static java.lang.System.getProperty;
import static java.lang.System.getenv;
import static org.apache.commons.lang3.StringUtils.firstNonBlank;

public final class KafkaEnvironmentProperties {

  private KafkaEnvironmentProperties() { }

  public static String port() {
    return firstNonBlank(getenv("KAFKA_PORT"), getProperty("kafka-port"), "9092");
  }

  public static String host() {
    return firstNonBlank(getenv("KAFKA_HOST"), getProperty("kafka-host"), "localhost");
  }

  public static String environment() {
    return firstNonBlank(getenv("ENV"), getProperty("env"), getProperty("environment"), "folio");
  }

  public static short replicationFactor() {
    return parseShort(firstNonBlank(getenv("REPLICATION_FACTOR"), getProperty("replication-factor"), "1"));
  }
}
