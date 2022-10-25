package org.folio.kafka.services;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class KafkaAdminClientServiceTest {

  private final Set<String> allExpectedTopics = Set.of(
    "folio.kafka-wrapper.foo-tenant.topic1",
    "folio.kafka-wrapper.foo-tenant.topic2",
    "folio.kafka-wrapper.foo-tenant.topic3"
  );

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
        assertThat(getTopicNames(createTopicsCaptor), containsInAnyOrder(allExpectedTopics.toArray()));
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

        assertThat(deleteTopicsCaptor.getAllValues().get(0), containsInAnyOrder(allExpectedTopics.toArray()));
      }));
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
        .createKafkaTopics("folio", "kafka-wrapper", "foo-tenant");
    }
  }

  private Future<Void> deleteKafkaTopicsAsync(KafkaAdminClient client) {
    try (var mocked = mockStatic(KafkaAdminClient.class)) {
      mocked.when(() -> KafkaAdminClient.create(eq(vertx), anyMap())).thenReturn(client);

      return new KafkaAdminClientService(vertx)
        .deleteKafkaTopics("folio", "kafka-wrapper", "foo-tenant");
    }
  }
}
