package com.example.batch.common.message.rabbitmq;

import com.example.batch.common.RetryPolicy;
import com.example.batch.common.message.Emitter;
import com.example.batch.common.message.EmitterProducer;
import com.example.batch.common.message.Message;
import com.example.batch.common.message.Receiver;
import com.example.batch.common.message.rabbitmq.RabbitMQReceiver.RetryHeaders;
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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Nikola Ivacic <nikola.ivacic@dropchop.com> on 05. 11. 2026.
 */
@QuarkusTest
@EnabledIfSystemProperty(named = "qmsg.rabbitmq.integration", matches = "true")
class RabbitMQScheduledRetryIntegrationTest {
  @Inject
  RabbitMQDeliveryApprover publisher;

  @Inject
  RabbitMQEmitterProducer emitterProducer;

  @Inject
  Receiver receiver;

  private Connection connection() throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(System.getProperty("quarkus.rabbitmqclient.hostname", "localhost"));
    factory.setPort(Integer.getInteger("quarkus.rabbitmqclient.port", 5672));
    factory.setUsername(System.getProperty("quarkus.rabbitmqclient.username", "guest"));
    factory.setPassword(System.getProperty("quarkus.rabbitmqclient.password", "guest"));
    return factory.newConnection();
  }

  @SuppressWarnings("BusyWait")
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

  @SuppressWarnings("DataFlowIssue")
  @Test
  void brokerSchedulesRetriesWithoutBlockingReadyMessagesAndPreservesMetadata() throws Exception {
    String service = "QmsgScheduledRetryTest." + UUID.randomUUID();
    String queue = "queue." + service;
    byte[] body = "original payload".getBytes(StandardCharsets.UTF_8);
    AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
        .messageId("stable-message-id").correlationId("stable-trace-id").deliveryMode(2).build();
    RetryPolicy retryPolicy = new RetryPolicy(2, List.of(2000L, 500L));
    try (RabbitMQEmitter emitter = emitterProducer.forService(service);
         Connection connection = connection(); Channel channel = connection.createChannel()) {
      publisher.useEmitter(emitter);
      try {
        RabbitMQQueues.declareWorkQueue(channel, queue);
        channel.confirmSelect();
        channel.basicPublish("", queue, properties, body);
        channel.waitForConfirmsOrDie(5000);
        GetResponse original = receive(channel, queue);

        long scheduledAt = System.currentTimeMillis();
        publisher.retry(queue, delivery(original), retryPolicy, 0, null);
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
            RetryHeaders.read(firstRetry.getProps().getHeaders()));

        publisher.retry(queue, delivery(firstRetry), retryPolicy, 0, null);
        channel.basicAck(firstRetry.getEnvelope().getDeliveryTag(), false);
        GetResponse secondRetry = receive(channel, queue);
        assertEquals(new Message.RetryMetadata(2, 2500, 500),
            RetryHeaders.read(secondRetry.getProps().getHeaders()));

        publisher.retry(queue, delivery(secondRetry), retryPolicy, 0, new IllegalStateException("still failing"));
        channel.basicAck(secondRetry.getEnvelope().getDeliveryTag(), false);
        GetResponse rejected = receive(channel, queue + ".dlq");
        assertArrayEquals(body, rejected.getBody());
        assertEquals(new Message.RetryMetadata(2, 2500, 500),
            RetryHeaders.read(rejected.getProps().getHeaders()));
        assertFalse(rejected.getProps().getHeaders().containsKey(RetryHeaders.DELIVERY_TIME_HEADER));
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

  @Test
  void interfacesResolveToCompatibleRabbitMqImplementationsAndDeliverMessages() throws Exception {
    String service = "QmsgReceiverInterfaceTest." + UUID.randomUUID();
    String queue = "queue." + service;
    AtomicBoolean stopped = new AtomicBoolean();
    AtomicBoolean recoveryRequested = new AtomicBoolean();
    AtomicReference<String> received = new AtomicReference<>();
    CountDownLatch processed = new CountDownLatch(1);
    Receiver.Controller controller = new Receiver.Controller() {
      public String queueName() { return queue; }
      public RetryPolicy retryPolicy() { return RetryPolicy.DEFAULT; }
      public boolean isRetryForbidden() { return stopped.get(); }
      public void clearConsumerTag() { }
      public void retryPending() { recoveryRequested.set(true); }
      public void retrySuccess() { }
      public void attemptStart() { recoveryRequested.set(true); }
    };
    EmitterProducer producer = emitterProducer;
    try (Emitter emitter = producer.forService(service); Connection connection = connection()) {
      String consumerTag = null;
      try {
        receiver.open(controller, emitter);
        Message.Reader<Message<String>, String> reader = body -> {
          Message<String> message = new Message<>();
          message.setPayload(new String(body, StandardCharsets.UTF_8));
          return message;
        };
        consumerTag = receiver.consume(reader, message -> {
          received.set(message.getPayload());
          processed.countDown();
          return Message.Result.acknowledged();
        });
        emitter.emit(null, "interface payload",
            message -> message.getPayload().toString().getBytes(StandardCharsets.UTF_8));
        assertTrue(processed.await(10, TimeUnit.SECONDS), "receiver did not process the emitted message");
        assertEquals("interface payload", received.get());
        assertTrue(receiver.isOpen());
        assertFalse(recoveryRequested.get());
      } finally {
        stopped.set(true);
        receiver.cancel(consumerTag);
        receiver.close();
        receiver.shutdownRetrier();
        deleteTestQueues(connection, queue, queue + ".dlq");
      }
    }
  }
}
