package org.folio.kafka.interceptors;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.folio.kafka.headers.FolioKafkaHeaders.TENANT_ID;

import java.util.Map;

/**
 * Kafka producer interceptor that will check the presence of a tenant Id header and log an error
 */
public class TenantIdCheckInterceptor implements ProducerInterceptor<String, String> {

  private static final Logger LOGGER = LogManager.getLogger();

  protected static final String TENANT_ID_ERROR_MESSAGE = "Kafka record does not have a tenant identifying header. " +
    "Use KafkaProducerRecordBuilder to build the record. TopicName={}";
  @Override
  public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
    Headers headers = record.headers();
    boolean isTenantIdHeaderExist = false;
    for (Header header : headers) {
      if (header.key().equals(TENANT_ID)) {
        isTenantIdHeaderExist = true;
        break;
      }
    }
    if (!isTenantIdHeaderExist) {
      LOGGER.error(TENANT_ID_ERROR_MESSAGE, record.topic());
    }

    return record;
  }

  @Override
  public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> configs) {

  }
}
