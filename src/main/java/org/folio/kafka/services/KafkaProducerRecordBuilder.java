package org.folio.kafka.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.folio.kafka.exception.ProducerCreationException;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.vertx.kafka.client.producer.KafkaProducerRecord.create;
import static java.util.Objects.isNull;
import static org.folio.okapi.common.XOkapiHeaders.TENANT;
import static org.folio.okapi.common.XOkapiHeaders.URL;

public final class KafkaProducerRecordBuilder<K, V> {
  private static final Set<String> FORWARDER_HEADERS = Set.of(URL.toLowerCase(), TENANT.toLowerCase());
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private V value;
  private K key;
  private String topic;
  private final Map<String, String> headers = new HashMap<>();

  static {
    MAPPER.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
  }

  public KafkaProducerRecordBuilder<K, V> value(V value) {
    this.value = value;
    return this;
  }

  public KafkaProducerRecordBuilder<K, V> key(K key) {
    this.key = key;
    return this;
  }

  public KafkaProducerRecordBuilder<K, V> topic(String topic) {
    this.topic = topic;
    return this;
  }

  public KafkaProducerRecordBuilder<K, V> header(String key, String value) {
    this.headers.put(key, value);
    return this;
  }

  public KafkaProducerRecordBuilder<K, V> propagateOkapiHeaders(Map<String, String> okapiHeaders) {
    okapiHeaders.entrySet().stream()
      .filter(entry -> FORWARDER_HEADERS.contains(entry.getKey().toLowerCase()))
      .forEach(entry -> header(entry.getKey(), entry.getValue()));

    return this;
  }

  public KafkaProducerRecord<K, String> build() {
    try {
      if (isNull(value)) throw new NullPointerException();
      var valueAsString = MAPPER.writeValueAsString(this.value);

      var kafkaProducerRecord = create(topic, key, valueAsString);
      headers.forEach(kafkaProducerRecord::addHeader);

      return kafkaProducerRecord;
    } catch (Exception ex) {
      throw new ProducerCreationException(ex.getMessage());
    }
  }
}
