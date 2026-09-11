package org.folio.kafka.filtering;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.folio.okapi.common.XOkapiHeaders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TenantEntitlementFilterTest {

  private static final String MODULE_ID = "mod-foo-1.0.0";

  private final Vertx vertx = Vertx.vertx();
  private final TenantEntitlementService service = mock(TenantEntitlementService.class);
  private final AtomicInteger initialLoadTriggerCount = new AtomicInteger();

  @AfterEach
  void tearDown() {
    vertx.close();
  }

  @Test
  void shouldSkip_shouldAccept_whenTenantIsEntitled() throws Exception {
    when(service.getEnabledTenants()).thenReturn(Set.of("diku"));
    var filter = filter(DisabledTenantStrategy.SKIP, DisabledTenantStrategy.FAIL);

    assertFalse(await(filter.shouldSkip(recordForTenant("diku"))));
  }

  @Test
  void shouldSkip_shouldAccept_whenTenantHeaderMissing() throws Exception {
    var filter = filter(DisabledTenantStrategy.SKIP, DisabledTenantStrategy.FAIL);

    assertFalse(await(filter.shouldSkip(recordWithHeaders(List.of()))));
  }

  @Test
  void shouldSkip_shouldSkip_whenTenantNotEntitledAndStrategyIsSkip() throws Exception {
    when(service.getEnabledTenants()).thenReturn(Set.of("college"));
    var filter = filter(DisabledTenantStrategy.SKIP, DisabledTenantStrategy.FAIL);

    assertTrue(await(filter.shouldSkip(recordForTenant("diku"))));
  }

  @Test
  void shouldSkip_shouldAccept_whenTenantNotEntitledAndStrategyIsAccept() throws Exception {
    when(service.getEnabledTenants()).thenReturn(Set.of("college"));
    var filter = filter(DisabledTenantStrategy.ACCEPT, DisabledTenantStrategy.FAIL);

    assertFalse(await(filter.shouldSkip(recordForTenant("diku"))));
  }

  @Test
  void shouldSkip_shouldFail_whenTenantNotEntitledAndStrategyIsFail() {
    when(service.getEnabledTenants()).thenReturn(Set.of("college"));
    var filter = filter(DisabledTenantStrategy.FAIL, DisabledTenantStrategy.FAIL);

    var future = filter.shouldSkip(recordForTenant("diku"));
    var exception = assertThrows(ExecutionException.class, () -> await(future));
    assertTrue(exception.getCause() instanceof TenantIsDisabledException);
  }

  @Test
  void shouldSkip_shouldApplyAllTenantsDisabledStrategy_whenNoTenantsEntitled() {
    when(service.getEnabledTenants()).thenReturn(Set.of());
    var filter = filter(DisabledTenantStrategy.SKIP, DisabledTenantStrategy.FAIL);

    var future = filter.shouldSkip(recordForTenant("diku"));
    var exception = assertThrows(ExecutionException.class, () -> await(future));
    assertTrue(exception.getCause() instanceof TenantsAreDisabledException);
  }

  @Test
  void shouldSkip_shouldAccept_whenNoTenantsEntitledAndAllTenantsStrategyIsAccept() throws Exception {
    when(service.getEnabledTenants()).thenReturn(Set.of());
    var filter = filter(DisabledTenantStrategy.SKIP, DisabledTenantStrategy.ACCEPT);

    assertFalse(await(filter.shouldSkip(recordForTenant("diku"))));
  }

  @Test
  void shouldSkip_shouldResolveQuickly_onceCachePopulatesDuringWait() throws Exception {
    var enabledTenants = new AtomicReference<Set<String>>(null);
    when(service.getEnabledTenants()).thenAnswer(invocation -> enabledTenants.get());
    var filter = filter(DisabledTenantStrategy.SKIP, DisabledTenantStrategy.FAIL);

    vertx.setTimer(300, id -> enabledTenants.set(Set.of("diku")));

    long startedAt = System.currentTimeMillis();
    var skipped = await(filter.shouldSkip(recordForTenant("diku")));
    long elapsedMs = System.currentTimeMillis() - startedAt;

    assertFalse(skipped);
    assertTrue(elapsedMs < 2000, "should resolve shortly after the cache populates, took " + elapsedMs + " ms");
    assertEquals(1, initialLoadTriggerCount.get());
  }

  @Test
  void shouldSkip_shouldAccept_whenCacheNeverPopulatesWithinTheBoundedWait() throws Exception {
    when(service.getEnabledTenants()).thenReturn(null);
    var filter = filter(DisabledTenantStrategy.SKIP, DisabledTenantStrategy.FAIL);

    assertFalse(await(filter.shouldSkip(recordForTenant("diku"))));
    assertEquals(1, initialLoadTriggerCount.get(), "initial load should be triggered exactly once");
  }

  private static boolean await(Future<Boolean> future) throws Exception {
    return future.toCompletionStage().toCompletableFuture().get(15, TimeUnit.SECONDS);
  }

  private TenantEntitlementFilter filter(DisabledTenantStrategy tenantDisabledStrategy,
    DisabledTenantStrategy allTenantsDisabledStrategy) {
    return new TenantEntitlementFilter(MODULE_ID, service, tenantDisabledStrategy, allTenantsDisabledStrategy,
      vertx, initialLoadTriggerCount::incrementAndGet);
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
