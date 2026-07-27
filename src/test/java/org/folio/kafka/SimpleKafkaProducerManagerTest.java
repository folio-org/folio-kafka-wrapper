package org.folio.kafka;

import static java.util.UUID.randomUUID;
import static org.folio.kafka.headers.FolioKafkaHeaders.TENANT_ID;
import static org.folio.kafka.services.TestKafkaTopic.TOPIC_ONE;
import static org.folio.okapi.common.XOkapiHeaders.REQUEST_ID;
import static org.folio.okapi.common.XOkapiHeaders.TENANT;
import static org.folio.okapi.common.XOkapiHeaders.TOKEN;
import static org.folio.okapi.common.XOkapiHeaders.URL;
import static org.folio.okapi.common.XOkapiHeaders.USER_ID;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaHeader;
import java.util.Map;
import org.folio.kafka.exception.ProducerCreationException;
import org.folio.kafka.services.KafkaProducerRecordBuilder;
import org.junit.jupiter.api.Test;

class SimpleKafkaProducerManagerTest {

  @Test
  void shouldReturnKafkaProduced() {
    KafkaConfig kafkaConfig = KafkaConfig.builder()
      .kafkaHost("localhost")
      .kafkaPort("9092")
      .build();

    SimpleKafkaProducerManager simpleKafkaProducerManager = new SimpleKafkaProducerManager(Vertx.vertx(), kafkaConfig);
    assertNotNull(simpleKafkaProducerManager.createShared("test_event"));
  }

  @Test
  void shouldBuildKafkaProducerRecord() {
    var expectedKey = randomUUID().toString();
    var expectedHeader = "okapi-header";
    var producerRecord = new KafkaProducerRecordBuilder<String, String>("tenant")
      .topic(TOPIC_ONE.topicName())
      .value(TOPIC_ONE.topicName())
      .key(expectedKey)
      .header(expectedHeader, expectedKey)
      .build();

    assertEquals(producerRecord.topic(), TOPIC_ONE.topicName());
    assertArrayEquals(new String[] {TENANT_ID, expectedHeader},
      producerRecord.headers().stream().map(KafkaHeader::key).toArray());
    assertEquals(producerRecord.key(), expectedKey);
    assertNotNull(producerRecord.value());
  }

  @Test
  void shouldPropagateOkapiHeaders() {
    String tenantId = "2";
    Map<String, String> okapiHeaders = Map.of(
      URL.toLowerCase(), "1",
      TENANT.toLowerCase(), tenantId,
      USER_ID.toLowerCase(), "user-id",
      REQUEST_ID.toLowerCase(), "request-id",
      TOKEN.toLowerCase(), "token",
      "not-okapi", "3");

    var producerRecord = new KafkaProducerRecordBuilder<String, String>(tenantId)
      .propagateOkapiHeaders(okapiHeaders)
      .value(TOPIC_ONE.topicName())
      .build();

    assertEquals(6, producerRecord.headers().size());
  }

  @Test
  @SuppressWarnings("java:S5778")
  void shouldFailToBuildNullValue() {
    assertThrows(ProducerCreationException.class, () ->
      new KafkaProducerRecordBuilder<String, String>("tenant")
        .value(null)
        .build());
  }

  @Test
  @SuppressWarnings("java:S5778")
  void shouldFailToBuildNullTenant() {
    assertThrows(ProducerCreationException.class, () ->
      new KafkaProducerRecordBuilder<String, String>(null)
        .value("test")
        .build());
  }
}
