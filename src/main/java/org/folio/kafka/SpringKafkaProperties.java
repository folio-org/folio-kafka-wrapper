package org.folio.kafka;

public final class SpringKafkaProperties {

  public static final String KAFKA_CONSUMER_AUTO_OFFSET_RESET = "spring.kafka.consumer.auto-offset-reset";

  public static final String KAFKA_CONSUMER_METADATA_MAX_AGE = "spring.kafka.consumer.properties.metadata.max.age.ms";

  public static final String KAFKA_CONSUMER_MAX_POLL_RECORDS = "spring.kafka.consumer.max-poll-records";

  public static final String KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS = "spring.kafka.consumer.properties.max.poll.interval.ms";

  public static final String KAFKA_CONSUMER_SESSION_TIMEOUT_MS = "spring.kafka.consumer.properties.session.timeout.ms";

  public static final String KAFKA_CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG = "spring.kafka.consumer.properties.heartbeat.interval.ms";

  public static final String KAFKA_SECURITY_PROTOCOL = "spring.kafka.security.protocol";

  public static final String KAFKA_PRODUCER_COMPRESSION_TYPE = "spring.kafka.producer.compression-type";

  public static final String KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS_CONFIG = "spring.kafka.producer.delivery.timeout.ms";

  public static final String KAFKA_PRODUCER_REQUEST_TIMEOUT_MS_CONFIG = "spring.kafka.producer.request.timeout.ms";

  public static final String KAFKA_PRODUCER_LINGER_MS_CONFIG = "spring.kafka.producer.linger.ms";

  public static final String KAFKA_PRODUCER_RETRY_BACKOFF_MS_CONFIG = "spring.kafka.producer.retry.backoff.ms";

  public static final String KAFKA_PRODUCER_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = "spring.kafka.producer.max.in.flight.requests.per.connection";

  public static final String KAFKA_PRODUCER_BATCH_SIZE_CONFIG = "spring.kafka.producer.batch.size";

  public static final String KAFKA_SSL_PROTOCOL = "spring.kafka.ssl.protocol";

  public static final String KAFKA_SSL_KEY_PASSWORD = "spring.kafka.ssl.key-password";

  public static final String KAFKA_SSL_TRUSTSTORE_LOCATION = "spring.kafka.ssl.trust-store-location";

  public static final String KAFKA_SSL_TRUSTSTORE_PASSWORD = "spring.kafka.ssl.trust-store-password";

  public static final String KAFKA_SSL_TRUSTSTORE_TYPE = "spring.kafka.ssl.trust-store-type";

  public static final String KAFKA_SSL_KEYSTORE_LOCATION = "spring.kafka.ssl.key-store-location";

  public static final String KAFKA_SSL_KEYSTORE_PASSWORD = "spring.kafka.ssl.key-store-password";

  public static final String KAFKA_SSL_KEYSTORE_TYPE = "spring.kafka.ssl.key-store-type";

  public static final String KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM = "spring.kafka.properties.ssl.endpoint.identification.algorithm";

  private SpringKafkaProperties() {
    throw new UnsupportedOperationException();
  }
}
