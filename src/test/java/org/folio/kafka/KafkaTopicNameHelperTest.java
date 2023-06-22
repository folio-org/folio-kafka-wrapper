package org.folio.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Test;

public class KafkaTopicNameHelperTest {

  @After
  public void tearDown(){
    // revert qualifier since KafkaTopicNameHelper is static and can affect other tests
    KafkaTopicNameHelper.setTenantCollectionTopicsQualifier(null);
  }

  @Test
  public void shouldFormatSubscriptionPatternForTenantAnySymbolWithAnyLength() {
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
  public void shouldBuildSubscriptionDefinition() {
    SubscriptionDefinition subscriptionDefinition = KafkaTopicNameHelper.createSubscriptionDefinition("folio", "Default", "DI_COMPLETED");
    assertNotNull(subscriptionDefinition);
    assertNotNull(subscriptionDefinition.getEventType());
    assertEquals( "DI_COMPLETED", subscriptionDefinition.getEventType());
    assertNotNull(subscriptionDefinition.getSubscriptionPattern());
    assertEquals("folio\\.Default\\.\\w{1,}\\.DI_COMPLETED", subscriptionDefinition.getSubscriptionPattern());
  }

  @Test
  public void shouldFormatGroupName() {
    String subscriptionDefinition = KafkaTopicNameHelper.formatGroupName("DI_COMPLETED", "folio-kafka-wrapper");
    assertNotNull(subscriptionDefinition);
    assertEquals("DI_COMPLETED.folio-kafka-wrapper", subscriptionDefinition);
  }

  @Test
  public void shouldGetEventTypeFromTopicName() {
    String eventType = KafkaTopicNameHelper.getEventTypeFromTopicName("folio.Default.test.DI_COMPLETED");
    assertNotNull(eventType);
    assertEquals("DI_COMPLETED", eventType);
  }

  @Test(expected = RuntimeException.class)
  public void shouldThrowRuntimeExceptionGetEventTypeFromTopicName() {
    KafkaTopicNameHelper.getEventTypeFromTopicName("folio,Default;test#DI_COMPLETED");
  }

  @Test
  public void shouldFormatTopicName() {
    String topicName = KafkaTopicNameHelper.formatTopicName("folio", "Default", "test","DI_COMPLETED");
    assertNotNull(topicName);
    assertEquals("folio.Default.test.DI_COMPLETED", topicName);

    // enable tenant collection topics
    KafkaTopicNameHelper.setTenantCollectionTopicsQualifier("COLLECTION");
    topicName = KafkaTopicNameHelper.formatTopicName("folio", "Default", "test","DI_COMPLETED");
    assertNotNull(topicName);
    assertEquals("folio.Default.COLLECTION.DI_COMPLETED", topicName);
  }

  @Test
  public void isTenantCollectionEnabled(){
    assertFalse(KafkaTopicNameHelper.isTenantCollectionTopicsEnabled());
    KafkaTopicNameHelper.setTenantCollectionTopicsQualifier("COLLECTION");
    assertTrue(KafkaTopicNameHelper.isTenantCollectionTopicsEnabled());
  }

  @Test(expected = RuntimeException.class)
  public void shouldErrorWhenBadTenantCollectionQualifier() {
      KafkaTopicNameHelper.setTenantCollectionTopicsQualifier("diku");
  }
}
