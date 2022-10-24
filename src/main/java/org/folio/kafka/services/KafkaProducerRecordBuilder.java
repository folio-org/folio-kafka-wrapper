package org.folio.kafka.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.folio.kafka.exception.ProducerCreationException;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.vertx.kafka.client.producer.KafkaProducerRecord.create;
import static org.folio.okapi.common.XOkapiHeaders.TENANT;
import static org.folio.okapi.common.XOkapiHeaders.URL;

public final class KafkaProducerRecordBuilder {
  private static final Set<String> FORWARDER_HEADERS = Set.of(URL.toLowerCase(), TENANT.toLowerCase());
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private Object value;
  private String key;
  private String topic;
  private final Map<String, String> headers = new HashMap<>();

  public KafkaProducerRecordBuilder value(Object value) {
    this.value = value;
    return this;
  }

  public KafkaProducerRecordBuilder key(String key) {
    this.key = key;
    return this;
  }

  public KafkaProducerRecordBuilder topic(String topic) {
    this.topic = topic;
    return this;
  }

  public KafkaProducerRecordBuilder header(String key, String value) {
    this.headers.put(key, value);
    return this;
  }

  public KafkaProducerRecordBuilder propagateOkapiHeaders(Map<String, String> okapiHeaders) {
    okapiHeaders.entrySet().stream()
      .filter(entry -> FORWARDER_HEADERS.contains(entry.getKey().toLowerCase()))
      .forEach(entry -> header(entry.getKey(), entry.getValue()));

    return this;
  }

  public KafkaProducerRecord<String, String> build() {
    try {
      var valueAsString = MAPPER.writeValueAsString(this.value);
      var kafkaProducerRecord = create(topic, key, valueAsString);
      headers.forEach(kafkaProducerRecord::addHeader);
      return kafkaProducerRecord;
    } catch (JsonProcessingException ex) {
      throw new ProducerCreationException();
    }
  }
}
