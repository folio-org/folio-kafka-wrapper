package org.folio.kafka.services;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.admin.NewTopic;
import org.apache.logging.log4j.Logger;
import org.folio.kafka.KafkaConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.folio.kafka.KafkaTopicNameHelper;

import static io.vertx.kafka.admin.KafkaAdminClient.create;
import static org.apache.logging.log4j.LogManager.getLogger;

public class KafkaAdminClientService {

  private static final Logger log = getLogger(KafkaAdminClientService.class);
  private final Supplier<KafkaAdminClient> clientFactory;

  public KafkaAdminClientService(Vertx vertx) {
    this.clientFactory = () -> create(vertx, KafkaConfig.builder()
      .kafkaHost(KafkaEnvironmentProperties.host())
      .kafkaPort(KafkaEnvironmentProperties.port())
      .build().getProducerProps());
  }

  public Future<Void> createKafkaTopics(KafkaTopic[] enumTopics, String tenantId) {
    final List<NewTopic> topics = readTopics(enumTopics, tenantId)
      .collect(Collectors.toList());

    return withKafkaAdminClient(adminClient -> createKafkaTopics(1, topics, adminClient))
      .onSuccess(result -> log.info("createKafkaTopics:: Topics created successfully"))
      .onFailure(cause -> log.error("createKafkaTopics:: Unable to create topics", cause));
  }

  public Future<Void> deleteKafkaTopics(KafkaTopic[] enumTopics, String tenantId) {
    if (KafkaTopicNameHelper.isTenantCollectionTopicsEnabled()) {
      return Future.succeededFuture();
    }
    List<String> topicsToDelete = readTopics(enumTopics, tenantId)
      .map(NewTopic::getName)
      .collect(Collectors.toList());

    return withKafkaAdminClient(kafkaAdminClient -> kafkaAdminClient.deleteTopics(topicsToDelete))
      .onSuccess(x -> log.info("deleteKafkaTopics:: Topics deleted successfully"))
      .onFailure(e -> log.error("deleteKafkaTopics:: Unable to delete topics", e));
  }

  private Future<Void> createKafkaTopics(int attempt, List<NewTopic> topics, KafkaAdminClient kafkaAdminClient) {
    return kafkaAdminClient.listTopics()
      .compose(existingTopics -> {
        final List<NewTopic> newTopics = new ArrayList<>(topics);
        newTopics.removeIf(t -> existingTopics.contains(t.getName()));
        if (newTopics.isEmpty()) {
          return Future.succeededFuture();
        }
        return kafkaAdminClient.createTopics(newTopics);
      })
      .recover(e -> {
        if (!(e instanceof org.apache.kafka.common.errors.TopicExistsException)) {
          return Future.failedFuture(e);
        }
        if (attempt >= 30) {
          log.error("createKafkaTopics:: attempt {} failed: {}", attempt, e.getMessage(), e);
          return Future.failedFuture(e);
        }
        log.info("createKafkaTopics:: Create topic attempt {} failed, sleeping and trying next attempt.", attempt);
        return sleep().compose(x -> createKafkaTopics(attempt + 1, topics, kafkaAdminClient));
      });
  }

  private <T> Future<T> withKafkaAdminClient(Function<KafkaAdminClient, Future<T>> function) {
    final KafkaAdminClient kafkaAdminClient = clientFactory.get();
    return function.apply(kafkaAdminClient)
      .eventually(x ->
        kafkaAdminClient.close()
          .onFailure(e -> log.error("withKafkaAdminClient:: Failed to close kafka admin client", e)));
  }

  private Stream<NewTopic> readTopics(KafkaTopic[] enumTopics, String tenant) {
    return Arrays.stream(enumTopics)
      .map(topic -> new NewTopic(
        topic.fullTopicName(tenant),
        topic.numPartitions(),
        topic.replicationFactor()));
  }

  /**
   * Sleep for 100 milliseconds.
   */
  private Future<Void> sleep() {
    Context context = Vertx.currentContext();
    Vertx vertx = context == null ? Vertx.vertx() : context.owner();
    return Future.future(promise -> vertx.setTimer(100, x -> promise.tryComplete()));
  }
}
