package org.folio.kafka.exception;

public class KafkaConfigValidationException extends RuntimeException {
  public KafkaConfigValidationException(String message) {
    super(message);
  }
}
