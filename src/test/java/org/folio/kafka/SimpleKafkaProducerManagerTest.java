package org.folio.kafka;

import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.folio.kafka.exception.ProducerCreationException;
import org.folio.kafka.services.KafkaProducerRecordBuilder;
import org.junit.Test;

import java.util.Map;

import static java.util.UUID.randomUUID;
import static org.folio.kafka.headers.FolioKafkaHeaders.TENANT_ID;
import static org.folio.kafka.services.TestKafkaTopic.TOPIC_ONE;
import static org.folio.okapi.common.XOkapiHeaders.TENANT;
import static org.folio.okapi.common.XOkapiHeaders.URL;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SimpleKafkaProducerManagerTest {

  @Test
  public void shouldReturnKafkaProduced() {
    KafkaConfig kafkaConfig = KafkaConfig.builder()
      .kafkaHost("localhost")
      .kafkaPort("9092")
      .build();

    SimpleKafkaProducerManager simpleKafkaProducerManager = new SimpleKafkaProducerManager(Vertx.vertx(), kafkaConfig);
    assertNotNull(simpleKafkaProducerManager.createShared("test_event"));
  }

  @Test
  public void shouldBuildKafkaProducerRecord() {
    var expectedKey = randomUUID().toString();
    var expectedHeader = "okapi-header";
    var producerRecord = new KafkaProducerRecordBuilder<String, String>("tenant")
      .topic(TOPIC_ONE.topicName())
      .value(TOPIC_ONE.topicName())
      .key(expectedKey)
      .header(expectedHeader, expectedKey)
      .build();

    assertEquals(producerRecord.topic(), TOPIC_ONE.topicName());
    assertArrayEquals(new String[]{TENANT_ID, expectedHeader},producerRecord.headers().stream().map(KafkaHeader::key).toArray());
    assertEquals(producerRecord.key(), expectedKey);
    assertNotNull(producerRecord.value());
  }

  @Test
  public void shouldPropagateOkapiHeaders() {
    String tenantId = "2";
    Map<String, String> okapiHeaders = Map.of(
      URL.toLowerCase(), "1",
      TENANT.toLowerCase(), tenantId,
      "not-okapi", "3");

    var producerRecord = new KafkaProducerRecordBuilder<String, String>(tenantId)
      .propagateOkapiHeaders(okapiHeaders)
      .value(TOPIC_ONE.topicName())
      .build();

    assertEquals(3, producerRecord.headers().size());
  }

  @Test(expected = ProducerCreationException.class)
  public void shouldFailToBuildNullValue() {
    new KafkaProducerRecordBuilder<String, String>("tenant")
      .value(null)
      .build();
  }

  @Test(expected = ProducerCreationException.class)
  public void shouldFailToBuildNullTenant() {
    new KafkaProducerRecordBuilder<String, String>(null)
      .value("test")
      .build();
  }
}
