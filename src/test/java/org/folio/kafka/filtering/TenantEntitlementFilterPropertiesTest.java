package org.folio.kafka.filtering;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TenantEntitlementFilterPropertiesTest {

  private static final String KAFKA_CONFIG_OKAPI_URL = "http://okapi:9130";

  @AfterEach
  void tearDown() {
    System.clearProperty(TenantEntitlementFilterProperties.OKAPI_URL);
  }

  @Test
  void okapiUrl_positive_usesConfiguredOkapiUrl() {
    System.setProperty(TenantEntitlementFilterProperties.OKAPI_URL, "http://localhost:8082");

    var result = TenantEntitlementFilterProperties.okapiUrl(KAFKA_CONFIG_OKAPI_URL);

    assertEquals("http://localhost:8082", result);
  }

  @Test
  void okapiUrl_positive_fallsBackToKafkaConfigOkapiUrl() {
    var result = TenantEntitlementFilterProperties.okapiUrl(KAFKA_CONFIG_OKAPI_URL);

    assertEquals(KAFKA_CONFIG_OKAPI_URL, result);
  }
}
