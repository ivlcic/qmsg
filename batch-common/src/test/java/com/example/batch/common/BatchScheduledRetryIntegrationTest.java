package com.example.batch.common;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.GetResponse;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@EnabledIfSystemProperty(named = "qmsg.rabbitmq.integration", matches = "true")
class BatchScheduledRetryIntegrationTest {
  @Inject
  BatchDeliveryPublisher publisher;

  @Inject
  BatchEmitter batchEmitter;

  private Connection connection() throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(System.getProperty("quarkus.rabbitmqclient.hostname", "localhost"));
    factory.setPort(Integer.getInteger("quarkus.rabbitmqclient.port", 5672));
    factory.setUsername(System.getProperty("quarkus.rabbitmqclient.username", "guest"));
    factory.setPassword(System.getProperty("quarkus.rabbitmqclient.password", "guest"));
    return factory.newConnection();
  }

  private GetResponse receive(Channel channel, String queue) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    do {
      GetResponse response = channel.basicGet(queue, false);
      if (response != null) {
        return response;
      }
      Thread.sleep(25);
    } while (System.nanoTime() < deadline);
    fail("No delivery received from " + queue);
    return null;
  }

  private Delivery delivery(GetResponse response) {
    return new Delivery(response.getEnvelope(), response.getProps(), response.getBody());
  }

  private void deleteTestQueues(Connection connection, String... queues) {
    for (String queue : queues) {
      try (Channel cleanup = connection.createChannel()) {
        cleanup.queueDelete(queue);
      } catch (Exception ignored) {
        // The queue may not exist if setup failed before declaration.
      }
    }
  }

  @Test
  void brokerSchedulesRetriesWithoutBlockingReadyMessagesAndPreservesMetadata() throws Exception {
    String service = "QmsgScheduledRetryTest." + UUID.randomUUID();
    String queue = "queue." + service;
    byte[] body = "original payload".getBytes(StandardCharsets.UTF_8);
    AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
        .messageId("stable-message-id").correlationId("stable-trace-id").deliveryMode(2).build();
    try (BatchEmitter.ServiceEmitter emitter = batchEmitter.forService(service);
         Connection connection = connection(); Channel channel = connection.createChannel()) {
      publisher.useEmitter(emitter);
      try {
        BatchQueues.declareWorkQueue(channel, queue);
        channel.confirmSelect();
        channel.basicPublish("", queue, properties, body);
        channel.waitForConfirmsOrDie(5000);
        GetResponse original = receive(channel, queue);

        long scheduledAt = System.currentTimeMillis();
        publisher.retry(queue, delivery(original), 2000, null);
        channel.basicAck(original.getEnvelope().getDeliveryTag(), false);
        emitter.emit(null, "ready message", message -> "ready message".getBytes(StandardCharsets.UTF_8));
        GetResponse ready = receive(channel, queue);
        assertEquals("ready message", new String(ready.getBody(), StandardCharsets.UTF_8));
        channel.basicAck(ready.getEnvelope().getDeliveryTag(), false);

        GetResponse firstRetry = receive(channel, queue);
        assertTrue(System.currentTimeMillis() >= scheduledAt + 2000, "scheduled delivery arrived too early");
        assertArrayEquals(body, firstRetry.getBody());
        assertEquals("stable-message-id", firstRetry.getProps().getMessageId());
        assertEquals("stable-trace-id", firstRetry.getProps().getCorrelationId());
        assertEquals(new Message.RetryMetadata(1, 2000, 2000),
            Message.RetryMetadata.fromHeaders(firstRetry.getProps().getHeaders()));

        publisher.retry(queue, delivery(firstRetry), 500, null);
        channel.basicAck(firstRetry.getEnvelope().getDeliveryTag(), false);
        GetResponse secondRetry = receive(channel, queue);
        assertEquals(new Message.RetryMetadata(2, 2500, 500),
            Message.RetryMetadata.fromHeaders(secondRetry.getProps().getHeaders()));

        publisher.deadLetter(queue, delivery(secondRetry), new IllegalArgumentException("permanent failure"));
        channel.basicAck(secondRetry.getEnvelope().getDeliveryTag(), false);
        GetResponse rejected = receive(channel, queue + ".dlq");
        assertArrayEquals(body, rejected.getBody());
        assertEquals(new Message.RetryMetadata(2, 2500, 500),
            Message.RetryMetadata.fromHeaders(rejected.getProps().getHeaders()));
        assertFalse(rejected.getProps().getHeaders().containsKey(Message.DELIVERY_TIME_HEADER));
        channel.basicAck(rejected.getEnvelope().getDeliveryTag(), false);

        publisher.close();
        emitter.emit(null, "still usable", message -> "still usable".getBytes(StandardCharsets.UTF_8));
        GetResponse afterClose = receive(channel, queue);
        assertEquals("still usable", new String(afterClose.getBody(), StandardCharsets.UTF_8));
        channel.basicAck(afterClose.getEnvelope().getDeliveryTag(), false);
      } finally {
        publisher.close();
        deleteTestQueues(connection, queue, queue + ".dlq");
      }
    }
  }
}
