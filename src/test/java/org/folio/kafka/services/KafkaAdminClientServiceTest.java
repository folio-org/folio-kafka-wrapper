package org.folio.kafka.services;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.folio.kafka.KafkaTopicNameHelper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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

@RunWith(VertxUnitRunner.class)
public class KafkaAdminClientServiceTest {

  private final List<String> allExpectedTopics = List.of(
    "folio.foo-tenant.kafka-wrapper.topic1",
    "folio.foo-tenant.kafka-wrapper.topic2",
    "folio.foo-tenant.kafka-wrapper.topic3"
  );

  private final String STUB_TENANT = "foo-tenant";
  private KafkaAdminClient mockClient;
  private Vertx vertx;

  @Before
  public void setUp() {
    vertx = mock(Vertx.class);
    mockClient = mock(KafkaAdminClient.class);
  }

  @Test
  public void shouldCreateTopicIfAlreadyExist(TestContext testContext) {
    when(mockClient.createTopics(anyList()))
      .thenReturn(failedFuture(new TopicExistsException("x")))
      .thenReturn(failedFuture(new TopicExistsException("y")))
      .thenReturn(failedFuture(new TopicExistsException("z")))
      .thenReturn(succeededFuture());
    when(mockClient.listTopics()).thenReturn(succeededFuture(Set.of("old")));
    when(mockClient.close()).thenReturn(succeededFuture());

    createKafkaTopicsAsync(mockClient)
      .onComplete(testContext.asyncAssertSuccess(notUsed -> {
        verify(mockClient, times(4)).listTopics();
        verify(mockClient, times(4)).createTopics(anyList());
        verify(mockClient, times(1)).close();
      }));
  }

  @Test
  public void shouldFailIfExistExceptionIsPermanent(TestContext testContext) {
    when(mockClient.createTopics(anyList())).thenReturn(failedFuture(new TopicExistsException("x")));
    when(mockClient.listTopics()).thenReturn(succeededFuture(Set.of("old")));
    when(mockClient.close()).thenReturn(succeededFuture());

    createKafkaTopicsAsync(mockClient)
      .onComplete(testContext.asyncAssertFailure(e -> {
        assertThat(e, instanceOf(TopicExistsException.class));
        verify(mockClient, times(1)).close();
      }));
  }

  @Test
  public void shouldNotCreateTopicOnOther(TestContext testContext) {
    when(mockClient.createTopics(anyList())).thenReturn(failedFuture(new RuntimeException("err msg")));
    when(mockClient.listTopics()).thenReturn(succeededFuture(Set.of("old")));
    when(mockClient.close()).thenReturn(succeededFuture());

    createKafkaTopicsAsync(mockClient)
      .onComplete(testContext.asyncAssertFailure(cause -> {
          testContext.assertEquals("err msg", cause.getMessage());
          verify(mockClient, times(1)).close();
        }
      ));
  }

  @Test
  public void shouldCreateTopicIfNotExist(TestContext testContext) {
    when(mockClient.createTopics(anyList())).thenReturn(succeededFuture());
    when(mockClient.listTopics()).thenReturn(succeededFuture(Set.of("old")));
    when(mockClient.close()).thenReturn(succeededFuture());

    createKafkaTopicsAsync(mockClient)
      .onComplete(testContext.asyncAssertSuccess(notUsed -> {

        @SuppressWarnings("unchecked") final ArgumentCaptor<List<NewTopic>> createTopicsCaptor = forClass(List.class);

        verify(mockClient, times(1)).createTopics(createTopicsCaptor.capture());
        verify(mockClient, times(1)).close();

        // Only these items are expected, so implicitly checks size of list
        assertTrue(allExpectedTopics.containsAll(getTopicNames(createTopicsCaptor)));

        var topicWithConfigs = createTopicsCaptor.getAllValues().get(0).stream()
          .filter(topic -> topic.getConfig() != null)
          .filter(topic -> Boolean.FALSE.equals(topic.getConfig().isEmpty()))
          .findFirst();
        assertTrue(topicWithConfigs.isPresent());
        assertEquals("1000",
          topicWithConfigs.get().getConfig().get(KafkaAdminClientService.MESSAGE_RETENTION_TIME_IN_MILLIS_CONFIG));
        assertEquals("2000",
          topicWithConfigs.get().getConfig().get(KafkaAdminClientService.MESSAGE_MAX_SIZE_IN_BYTES_CONFIG));
      }));
  }

  @Test
  public void shouldDeleteTopics(TestContext testContext) {
    when(mockClient.deleteTopics(anyList())).thenReturn(succeededFuture());
    when(mockClient.close()).thenReturn(succeededFuture());

    deleteKafkaTopicsAsync(mockClient)
      .onComplete(testContext.asyncAssertSuccess(notUsed -> {

        @SuppressWarnings("unchecked") final ArgumentCaptor<List<String>> deleteTopicsCaptor = forClass(List.class);

        verify(mockClient, times(1)).deleteTopics(deleteTopicsCaptor.capture());
        verify(mockClient, times(1)).close();

        assertTrue(allExpectedTopics.containsAll(deleteTopicsCaptor.getAllValues().get(0)));
      }));
  }

  @Test
  public void shouldNotDeleteTopics_whenCollectionTopicIsEnabled(TestContext testContext) {
    try (var mocked = mockStatic(KafkaTopicNameHelper.class)) {
      mocked.when(KafkaTopicNameHelper::isTenantCollectionTopicsEnabled).thenReturn(true);

      new KafkaAdminClientService(vertx)
        .deleteKafkaTopics(TestKafkaTopic.values(), STUB_TENANT)
        .onComplete(testContext.asyncAssertSuccess(notUsed -> verifyNoInteractions(mockClient)));
    }
  }

  private List<String> getTopicNames(ArgumentCaptor<List<NewTopic>> createTopicsCaptor) {
    return createTopicsCaptor.getAllValues().get(0).stream()
      .map(NewTopic::getName)
      .collect(Collectors.toList());
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
