package org.folio.kafka.filtering;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import java.util.List;
import java.util.Set;
import org.folio.okapi.common.XOkapiHeaders;
import org.junit.jupiter.api.Test;

class TenantEntitlementFilterTest {

  private static final String MODULE_ID = "mod-foo-1.0.0";

  private final TenantEntitlementService service = mock(TenantEntitlementService.class);

  @Test
  void shouldSkip_shouldAccept_whenTenantIsEntitled() {
    when(service.getEnabledTenants()).thenReturn(Set.of("diku"));
    var filter = filter(DisabledTenantStrategy.SKIP, DisabledTenantStrategy.FAIL);

    assertFalse(filter.shouldSkip(recordForTenant("diku")));
  }

  @Test
  void shouldSkip_shouldAccept_whenTenantHeaderMissing() {
    var filter = filter(DisabledTenantStrategy.SKIP, DisabledTenantStrategy.FAIL);

    assertFalse(filter.shouldSkip(recordWithHeaders(List.of())));
  }

  @Test
  void shouldSkip_shouldAccept_whenCacheNotYetPopulated() {
    when(service.getEnabledTenants()).thenReturn(null);
    var filter = filter(DisabledTenantStrategy.SKIP, DisabledTenantStrategy.FAIL);

    assertFalse(filter.shouldSkip(recordForTenant("diku")));
  }

  @Test
  void shouldSkip_shouldSkip_whenTenantNotEntitledAndStrategyIsSkip() {
    when(service.getEnabledTenants()).thenReturn(Set.of("college"));
    var filter = filter(DisabledTenantStrategy.SKIP, DisabledTenantStrategy.FAIL);

    assertTrue(filter.shouldSkip(recordForTenant("diku")));
  }

  @Test
  void shouldSkip_shouldAccept_whenTenantNotEntitledAndStrategyIsAccept() {
    when(service.getEnabledTenants()).thenReturn(Set.of("college"));
    var filter = filter(DisabledTenantStrategy.ACCEPT, DisabledTenantStrategy.FAIL);

    assertFalse(filter.shouldSkip(recordForTenant("diku")));
  }

  @Test
  void shouldSkip_shouldThrow_whenTenantNotEntitledAndStrategyIsFail() {
    when(service.getEnabledTenants()).thenReturn(Set.of("college"));
    var filter = filter(DisabledTenantStrategy.FAIL, DisabledTenantStrategy.FAIL);

    assertThrows(TenantIsDisabledException.class, () -> filter.shouldSkip(recordForTenant("diku")));
  }

  @Test
  void shouldSkip_shouldApplyAllTenantsDisabledStrategy_whenNoTenantsEntitled() {
    when(service.getEnabledTenants()).thenReturn(Set.of());
    var filter = filter(DisabledTenantStrategy.SKIP, DisabledTenantStrategy.FAIL);

    assertThrows(TenantsAreDisabledException.class, () -> filter.shouldSkip(recordForTenant("diku")));
  }

  @Test
  void shouldSkip_shouldAccept_whenNoTenantsEntitledAndAllTenantsStrategyIsAccept() {
    when(service.getEnabledTenants()).thenReturn(Set.of());
    var filter = filter(DisabledTenantStrategy.SKIP, DisabledTenantStrategy.ACCEPT);

    assertFalse(filter.shouldSkip(recordForTenant("diku")));
  }

  private TenantEntitlementFilter filter(DisabledTenantStrategy tenantDisabledStrategy,
    DisabledTenantStrategy allTenantsDisabledStrategy) {
    return new TenantEntitlementFilter(MODULE_ID, service, tenantDisabledStrategy, allTenantsDisabledStrategy);
  }

  @SuppressWarnings("unchecked")
  private KafkaConsumerRecord<String, String> recordForTenant(String tenant) {
    return recordWithHeaders(List.of(KafkaHeader.header(XOkapiHeaders.TENANT, tenant)));
  }

  @SuppressWarnings("unchecked")
  private KafkaConsumerRecord<String, String> recordWithHeaders(List<KafkaHeader> headers) {
    KafkaConsumerRecord<String, String> record = mock(KafkaConsumerRecord.class);
    when(record.headers()).thenReturn(headers);
    return record;
  }
}
