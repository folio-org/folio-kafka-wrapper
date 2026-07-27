package org.folio.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.vertx.core.MultiMap;
import io.vertx.kafka.client.producer.KafkaHeader;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class KafkaHeaderUtilsTest {

  @Test
  void shouldReturnDistinctValuesInListWhenThereAreDuplicateElements() {
    MultiMap headers = MultiMap.caseInsensitiveMultiMap();
    headers.add("x-okapi-request-method", "POST");
    headers.add("x-okapi-request-method", "POST");
    List<KafkaHeader> kafkaHeaders = KafkaHeaderUtils.kafkaHeadersFromMultiMap(headers);
    assertEquals(1, kafkaHeaders.size());
  }

  @Test
  void shouldConvertMapToKafkaHeaders() {
    Map<String, String> headers = Map.of(
      "x-okapi-tenant", "diku",
      "x-okapi-user-id", UUID.randomUUID().toString());

    List<KafkaHeader> kafkaHeaders = KafkaHeaderUtils.kafkaHeadersFromMap(headers);

    assertEquals(2, kafkaHeaders.size());
    for (Map.Entry<String, String> entry : headers.entrySet()) {
      KafkaHeader header = getKafkaHeader(entry.getKey(), kafkaHeaders);
      assertNotNull(header);
      assertEquals(header.value().toString(), entry.getValue());
    }
  }

  @Test
  void shouldConvertKafkaHeadersToMap() {
    List<KafkaHeader> kafkaHeaders = List.of(
      KafkaHeader.header("x-okapi-tenant", "diku"),
      KafkaHeader.header("x-okapi-user-id", UUID.randomUUID().toString()));

    Map<String, String> headersMap = KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders);

    assertEquals(2, headersMap.size());
    for (KafkaHeader kafkaHeader : kafkaHeaders) {
      assertNotNull(headersMap.get(kafkaHeader.key()));
      assertEquals(headersMap.get(kafkaHeader.key()), kafkaHeader.value().toString());
    }
  }

  private KafkaHeader getKafkaHeader(String headerName, List<KafkaHeader> kafkaHeaders) {
    return kafkaHeaders.stream()
      .filter(h -> h.key().equals(headerName))
      .findFirst()
      .orElse(null);
  }
}
