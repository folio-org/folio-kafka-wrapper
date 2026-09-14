package org.folio.kafka.filtering;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import io.vertx.core.Vertx;
import org.folio.kafka.KafkaConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TenantEntitlementFilterProviderTest {

  private static final String MODULE_ID = "mod-foo-1.0.0";

  private final Vertx vertx = Vertx.vertx();
  private final KafkaConfig kafkaConfig = KafkaConfig.builder()
    .kafkaHost("localhost")
    .kafkaPort("9092")
    .okapiUrl("http://localhost:9130")
    .build();

  @BeforeEach
  void setUp() {
    System.setProperty(TenantEntitlementFilterProperties.ENABLED, "true");
  }

  @AfterEach
  void tearDown() {
    System.clearProperty(TenantEntitlementFilterProperties.ENABLED);
    vertx.close();
  }

  @Test
  void getOrCreate_shouldReturnSameFilterInstance_forSameModuleId() {
    var filter1 = TenantEntitlementFilterProvider.getOrCreate(vertx, kafkaConfig, MODULE_ID);
    var filter2 = TenantEntitlementFilterProvider.getOrCreate(vertx, kafkaConfig, MODULE_ID);

    assertSame(filter1, filter2, "same moduleId should reuse the same shared filter");
  }

  @Test
  void getOrCreate_shouldReturnDifferentFilterInstance_forDifferentModuleId() {
    var filter1 = TenantEntitlementFilterProvider.getOrCreate(vertx, kafkaConfig, MODULE_ID);
    var filter2 = TenantEntitlementFilterProvider.getOrCreate(vertx, kafkaConfig, "mod-bar-1.0.0");

    assertNotSame(filter1, filter2, "different moduleId should get its own filter");
  }

  @Test
  void getOrCreate_shouldReturnNull_whenFilteringDisabled() {
    System.clearProperty(TenantEntitlementFilterProperties.ENABLED);

    var filter = TenantEntitlementFilterProvider.getOrCreate(vertx, kafkaConfig, MODULE_ID);

    assertNull(filter);
  }
}
