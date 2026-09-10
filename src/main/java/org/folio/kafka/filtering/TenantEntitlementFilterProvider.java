package org.folio.kafka.filtering;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.services.KafkaEnvironmentProperties;

/**
 * Lazily creates and shares one {@link TenantEntitlementFilter} per module id within this JVM, so
 * that any number of {@code KafkaConsumerWrapper} instances started by the same module reuse a
 * single entitlement cache and a single {@code entitlement}-topic consumer, rather than each
 * spinning up their own.
 *
 * <p>Returns {@code null} when filtering is disabled via {@link TenantEntitlementFilterProperties},
 * which is the case unless a module opts in.
 */
public final class TenantEntitlementFilterProvider {

  private static final Logger LOGGER = LogManager.getLogger();
  private static final Map<String, TenantEntitlementFilter> FILTERS = new ConcurrentHashMap<>();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private TenantEntitlementFilterProvider() {
  }

  /**
   * Returns the shared entitlement filter for the given module, creating it on first call.
   *
   * @param vertx the Vertx instance
   * @param kafkaConfig Kafka client configuration, also used for {@code okapiUrl}
   * @param moduleId current module id, for example {@code mod-foo-1.0.0}
   * @return the shared filter, or {@code null} if filtering is disabled or {@code moduleId} is blank
   */
  public static TenantEntitlementFilter getOrCreate(Vertx vertx, KafkaConfig kafkaConfig, String moduleId) {
    if (!TenantEntitlementFilterProperties.enabled()) {
      return null;
    }

    if (StringUtils.isBlank(moduleId)) {
      LOGGER.warn("getOrCreate:: Tenant entitlement filtering is enabled but moduleId is blank; "
        + "skipping filter setup");
      return null;
    }

    return FILTERS.computeIfAbsent(moduleId, id -> createFilter(vertx, kafkaConfig, id));
  }

  private static TenantEntitlementFilter createFilter(Vertx vertx, KafkaConfig kafkaConfig, String moduleId) {
    LOGGER.info("createFilter:: Setting up tenant entitlement filter: moduleId = {}", moduleId);

    var client = new WebClientTenantEntitlementClient(vertx, kafkaConfig.getOkapiUrl());
    var service = new TenantEntitlementService(moduleId, client);
    service.refresh();

    startEntitlementEventConsumer(vertx, kafkaConfig, moduleId, service);
    vertx.setPeriodic(TenantEntitlementFilterProperties.entitlementRefreshIntervalMs(),
      timerId -> service.refresh());

    return new TenantEntitlementFilter(moduleId, service,
      TenantEntitlementFilterProperties.tenantDisabledStrategy(),
      TenantEntitlementFilterProperties.allTenantsDisabledStrategy());
  }

  private static void startEntitlementEventConsumer(Vertx vertx, KafkaConfig kafkaConfig, String moduleId,
    TenantEntitlementService service) {
    var consumerProps = kafkaConfig.getConsumerProps();
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-tenant-filter-entitlement-" + UUID.randomUUID());
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

    KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, consumerProps);
    consumer.handler(record -> handleEntitlementRecord(record.value(), moduleId, service));
    consumer.exceptionHandler(throwable ->
      LOGGER.error("startEntitlementEventConsumer:: Error consuming entitlement topic: moduleId = {}",
        moduleId, throwable));

    var topic = KafkaEnvironmentProperties.environment() + ".entitlement";
    consumer.subscribe(topic)
      .onSuccess(ar -> LOGGER.info("startEntitlementEventConsumer:: Subscribed to {}: moduleId = {}",
        topic, moduleId))
      .onFailure(throwable -> LOGGER.error(
        "startEntitlementEventConsumer:: Failed to subscribe to {}: moduleId = {}", topic, moduleId, throwable));
  }

  private static void handleEntitlementRecord(String value, String moduleId, TenantEntitlementService service) {
    try {
      var event = OBJECT_MAPPER.readValue(value, EntitlementEvent.class);
      service.applyEntitlementEvent(event);
    } catch (Exception e) {
      LOGGER.warn("handleEntitlementRecord:: Failed to parse entitlement event: moduleId = {}, value = {}",
        moduleId, value, e);
    }
  }
}
