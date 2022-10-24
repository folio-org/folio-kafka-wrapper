package org.folio.kafka.exception;

public class KafkaTopicsFileReadException extends RuntimeException {
  private static final String MESSAGE = "Failed to read file '%s' from resources";

  public KafkaTopicsFileReadException(String fileName) {
    super(String.format(MESSAGE, fileName));
  }
}
