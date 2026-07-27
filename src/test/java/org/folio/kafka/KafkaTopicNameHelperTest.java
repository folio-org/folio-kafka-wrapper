package org.folio.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class KafkaTopicNameHelperTest {

  @AfterEach
  void tearDown() {
    // revert qualifier since KafkaTopicNameHelper is static and can affect other tests
    KafkaTopicNameHelper.setTenantCollectionTopicsQualifier(null);
  }

  @Test
  void shouldFormatSubscriptionPatternForTenantAnySymbolWithAnyLength() {
    String subscriptionPattern = KafkaTopicNameHelper.formatSubscriptionPattern("folio", "Default", "DI_COMPLETED");
    Pattern pattern = Pattern.compile(subscriptionPattern);
    assertNotNull(subscriptionPattern);
    assertTrue(pattern.matcher("folio.Default.test.DI_COMPLETED").matches());
    assertTrue(pattern.matcher("folio.Default.tes.DI_COMPLETED").matches());
    assertTrue(pattern.matcher("folio.Default.te.DI_COMPLETED").matches());
    assertTrue(pattern.matcher("folio.Default.t.DI_COMPLETED").matches());
    assertTrue(pattern.matcher("folio.Default.t1.DI_COMPLETED").matches());
    assertTrue(pattern.matcher("folio.Default.1.DI_COMPLETED").matches());
    assertTrue(pattern.matcher("folio.Default.1.DI_COMPLETED").matches());
  }

  @Test
  void shouldBuildSubscriptionDefinition() {
    SubscriptionDefinition subscriptionDefinition =
      KafkaTopicNameHelper.createSubscriptionDefinition("folio", "Default", "DI_COMPLETED");
    assertNotNull(subscriptionDefinition);
    assertNotNull(subscriptionDefinition.getEventType());
    assertEquals("DI_COMPLETED", subscriptionDefinition.getEventType());
    assertNotNull(subscriptionDefinition.getSubscriptionPattern());
    assertEquals("folio\\.Default\\.\\w{1,}\\.DI_COMPLETED", subscriptionDefinition.getSubscriptionPattern());
  }

  @Test
  void shouldFormatGroupName() {
    String subscriptionDefinition = KafkaTopicNameHelper.formatGroupName("DI_COMPLETED", "folio-kafka-wrapper");
    assertNotNull(subscriptionDefinition);
    assertEquals("DI_COMPLETED.folio-kafka-wrapper", subscriptionDefinition);
  }

  @Test
  void shouldGetEventTypeFromTopicName() {
    String eventType = KafkaTopicNameHelper.getEventTypeFromTopicName("folio.Default.test.DI_COMPLETED");
    assertNotNull(eventType);
    assertEquals("DI_COMPLETED", eventType);
  }

  @Test
  void shouldThrowRuntimeExceptionGetEventTypeFromTopicName() {
    assertThrows(RuntimeException.class,
      () -> KafkaTopicNameHelper.getEventTypeFromTopicName("folio,Default;test#DI_COMPLETED"));
  }

  @Test
  void shouldFormatTopicName() {
    String topicName = KafkaTopicNameHelper.formatTopicName("folio", "Default", "test", "DI_COMPLETED");
    assertNotNull(topicName);
    assertEquals("folio.Default.test.DI_COMPLETED", topicName);

    // enable tenant collection topics
    KafkaTopicNameHelper.setTenantCollectionTopicsQualifier("COLLECTION");
    topicName = KafkaTopicNameHelper.formatTopicName("folio", "Default", "test", "DI_COMPLETED");
    assertNotNull(topicName);
    assertEquals("folio.Default.COLLECTION.DI_COMPLETED", topicName);
  }

  @Test
  void shouldFormatTopicNameWithoutNamespace() {
    String topicName = KafkaTopicNameHelper.formatTopicName("folio", "test", "DI_COMPLETED");
    assertNotNull(topicName);
    assertEquals("folio.test.DI_COMPLETED", topicName);

    // enable tenant collection topics
    KafkaTopicNameHelper.setTenantCollectionTopicsQualifier("COLLECTION");
    topicName = KafkaTopicNameHelper.formatTopicName("folio", "test", "DI_COMPLETED");
    assertNotNull(topicName);
    assertEquals("folio.COLLECTION.DI_COMPLETED", topicName);
  }

  @Test
  void getDefaultNamespace() {
    assertEquals("Default", KafkaTopicNameHelper.getDefaultNameSpace());
  }

  @Test
  void isTenantCollectionEnabled() {
    assertFalse(KafkaTopicNameHelper.isTenantCollectionTopicsEnabled());
    KafkaTopicNameHelper.setTenantCollectionTopicsQualifier("COLLECTION");
    assertTrue(KafkaTopicNameHelper.isTenantCollectionTopicsEnabled());
  }

  @Test
  void shouldErrorWhenBadTenantCollectionQualifier() {
    assertThrows(RuntimeException.class, () -> KafkaTopicNameHelper.setTenantCollectionTopicsQualifier("diku"));
  }
}
