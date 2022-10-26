package org.folio.kafka;

import io.vertx.core.Vertx;
import org.folio.kafka.exception.ProducerCreationException;
import org.folio.kafka.services.KafkaProducerRecordBuilder;
import org.junit.Test;

import java.util.Map;

import static java.util.UUID.randomUUID;
import static org.folio.kafka.services.TestKafkaTopic.TOPIC_ONE;
import static org.folio.okapi.common.XOkapiHeaders.TENANT;
import static org.folio.okapi.common.XOkapiHeaders.URL;
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
    var producerRecord = new KafkaProducerRecordBuilder<String, String>()
      .topic(TOPIC_ONE.topicName())
      .value(TOPIC_ONE.topicName())
      .key(expectedKey)
      .header(expectedHeader, expectedKey)
      .build();

    assertEquals(producerRecord.topic(), TOPIC_ONE.topicName());
    assertEquals(producerRecord.headers().get(0).key(), expectedHeader);
    assertEquals(producerRecord.key(), expectedKey);
    assertNotNull(producerRecord.value());
  }

  @Test
  public void shouldPropagateOkapiHeaders() {
    Map<String, String> okapiHeaders = Map.of(
      URL.toLowerCase(), "1",
      TENANT.toLowerCase(), "2",
      "not-okapi", "3");

    var producerRecord = new KafkaProducerRecordBuilder<String, String>()
      .propagateOkapiHeaders(okapiHeaders)
      .value(TOPIC_ONE.topicName())
      .build();

    assertEquals(2, producerRecord.headers().size());
  }

  @Test(expected = ProducerCreationException.class)
  public void shouldFailToBuildNullValue() {
    new KafkaProducerRecordBuilder<String, String>()
      .value(null)
      .build();
  }
}
