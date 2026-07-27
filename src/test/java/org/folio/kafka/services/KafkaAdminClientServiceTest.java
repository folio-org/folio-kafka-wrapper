package org.folio.kafka.services;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.admin.NewTopic;
import java.util.List;
import java.util.Set;
import org.apache.kafka.common.errors.TopicExistsException;
import org.folio.kafka.KafkaTopicNameHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

@ExtendWith(VertxExtension.class)
class KafkaAdminClientServiceTest {

  private static final String STUB_TENANT = "foo-tenant";

  private final List<String> allExpectedTopics = List.of(
    "folio.foo-tenant.kafka-wrapper.topic1",
    "folio.foo-tenant.kafka-wrapper.topic2",
    "folio.foo-tenant.kafka-wrapper.topic3"
  );

  private KafkaAdminClient mockClient;
  private Vertx vertx;

  @BeforeEach
  void setUp() {
    vertx = mock(Vertx.class);
    mockClient = mock(KafkaAdminClient.class);
  }

  @Test
  void shouldCreateTopicIfAlreadyExist(VertxTestContext testContext) {
    when(mockClient.createTopics(anyList()))
      .thenReturn(failedFuture(new TopicExistsException("x")))
      .thenReturn(failedFuture(new TopicExistsException("y")))
      .thenReturn(failedFuture(new TopicExistsException("z")))
      .thenReturn(succeededFuture());
    when(mockClient.listTopics()).thenReturn(succeededFuture(Set.of("old")));
    when(mockClient.close()).thenReturn(succeededFuture());

    createKafkaTopicsAsync(mockClient)
      .onComplete(testContext.succeeding(notUsed -> testContext.verify(() -> {
        verify(mockClient, times(4)).listTopics();
        verify(mockClient, times(4)).createTopics(anyList());
        verify(mockClient, times(1)).close();
        testContext.completeNow();
      })));
  }

  @Test
  void shouldFailIfExistExceptionIsPermanent(VertxTestContext testContext) {
    when(mockClient.createTopics(anyList())).thenReturn(failedFuture(new TopicExistsException("x")));
    when(mockClient.listTopics()).thenReturn(succeededFuture(Set.of("old")));
    when(mockClient.close()).thenReturn(succeededFuture());

    createKafkaTopicsAsync(mockClient)
      .onComplete(testContext.failing(e -> testContext.verify(() -> {
        assertThat(e, instanceOf(TopicExistsException.class));
        verify(mockClient, times(1)).close();
        testContext.completeNow();
      })));
  }

  @Test
  void shouldNotCreateTopicOnOther(VertxTestContext testContext) {
    when(mockClient.createTopics(anyList())).thenReturn(failedFuture(new RuntimeException("err msg")));
    when(mockClient.listTopics()).thenReturn(succeededFuture(Set.of("old")));
    when(mockClient.close()).thenReturn(succeededFuture());

    createKafkaTopicsAsync(mockClient)
      .onComplete(testContext.failing(cause -> testContext.verify(
        () -> {
          assertEquals("err msg", cause.getMessage());
          verify(mockClient, times(1)).close();
          testContext.completeNow();
        }
      )));
  }

  @Test
  void shouldCreateTopicIfNotExist(VertxTestContext testContext) {
    when(mockClient.createTopics(anyList())).thenReturn(succeededFuture());
    when(mockClient.listTopics()).thenReturn(succeededFuture(Set.of("old")));
    when(mockClient.close()).thenReturn(succeededFuture());

    createKafkaTopicsAsync(mockClient)
      .onComplete(testContext.succeeding(notUsed -> testContext.verify(() -> {

        @SuppressWarnings("unchecked") final ArgumentCaptor<List<NewTopic>> createTopicsCaptor = forClass(List.class);

        verify(mockClient, times(1)).createTopics(createTopicsCaptor.capture());
        verify(mockClient, times(1)).close();

        // Only these items are expected, so implicitly checks size of list
        assertTrue(allExpectedTopics.containsAll(getTopicNames(createTopicsCaptor)));

        var topicWithConfigs = createTopicsCaptor.getAllValues().getFirst().stream()
          .filter(topic -> topic.getConfig() != null)
          .filter(topic -> !topic.getConfig().isEmpty())
          .findFirst();
        assertTrue(topicWithConfigs.isPresent());
        assertEquals(TestKafkaTopic.TOPIC_THREE.messageRetentionTime() + "",
          topicWithConfigs.get().getConfig().get(KafkaAdminClientService.MESSAGE_RETENTION_TIME_IN_MILLIS_CONFIG));
        assertEquals(TestKafkaTopic.TOPIC_THREE.messageMaxSize() + "",
          topicWithConfigs.get().getConfig().get(KafkaAdminClientService.MESSAGE_MAX_SIZE_IN_BYTES_CONFIG));
        testContext.completeNow();
      })));
  }

  @Test
  void shouldDeleteTopics(VertxTestContext testContext) {
    when(mockClient.deleteTopics(anyList())).thenReturn(succeededFuture());
    when(mockClient.close()).thenReturn(succeededFuture());

    deleteKafkaTopicsAsync(mockClient)
      .onComplete(testContext.succeeding(notUsed -> testContext.verify(() -> {

        @SuppressWarnings("unchecked") final ArgumentCaptor<List<String>> deleteTopicsCaptor = forClass(List.class);

        verify(mockClient, times(1)).deleteTopics(deleteTopicsCaptor.capture());
        verify(mockClient, times(1)).close();

        assertTrue(allExpectedTopics.containsAll(deleteTopicsCaptor.getAllValues().getFirst()));
        testContext.completeNow();
      })));
  }

  @Test
  void shouldNotDeleteTopics_whenCollectionTopicIsEnabled(VertxTestContext testContext) {
    try (var mocked = mockStatic(KafkaTopicNameHelper.class)) {
      mocked.when(KafkaTopicNameHelper::isTenantCollectionTopicsEnabled).thenReturn(true);

      new KafkaAdminClientService(vertx)
        .deleteKafkaTopics(TestKafkaTopic.values(), STUB_TENANT)
        .onComplete(testContext.succeeding(notUsed -> testContext.verify(() -> {
          verifyNoInteractions(mockClient);
          testContext.completeNow();
        })));
    }
  }

  private List<String> getTopicNames(ArgumentCaptor<List<NewTopic>> createTopicsCaptor) {
    return createTopicsCaptor.getAllValues().getFirst().stream()
      .map(NewTopic::getName)
      .toList();
  }

  private Future<Void> createKafkaTopicsAsync(KafkaAdminClient client) {
    try (var mocked = mockStatic(KafkaAdminClient.class)) {
      mocked.when(() -> KafkaAdminClient.create(eq(vertx), anyMap())).thenReturn(client);

      return new KafkaAdminClientService(vertx)
        .createKafkaTopics(TestKafkaTopic.values(), STUB_TENANT);
    }
  }

  private Future<Void> deleteKafkaTopicsAsync(KafkaAdminClient client) {
    try (var mocked = mockStatic(KafkaAdminClient.class)) {
      mocked.when(() -> KafkaAdminClient.create(eq(vertx), anyMap())).thenReturn(client);

      return new KafkaAdminClientService(vertx)
        .deleteKafkaTopics(TestKafkaTopic.values(), STUB_TENANT);
    }
  }
}
