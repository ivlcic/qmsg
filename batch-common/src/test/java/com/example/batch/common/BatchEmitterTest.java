package com.example.batch.common;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import io.quarkiverse.rabbitmqclient.RabbitMQClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class BatchEmitterTest {
  @FunctionalInterface
  private interface Confirmation {
    void await() throws Exception;
  }

  private static class PublishingConnection {
    final List<PublishingChannel> channels = new CopyOnWriteArrayList<>();
    final Connection proxy;
    boolean open = true;
    boolean aborted;

    PublishingConnection() {
      proxy = (Connection) Proxy.newProxyInstance(
          Connection.class.getClassLoader(), new Class<?>[]{Connection.class}, (instance, method, args) -> {
            switch (method.getName()) {
              case "isOpen": return open;
              case "getServerProperties": return Map.of("version", "4.4.0");
              case "createChannel":
                PublishingChannel channel = new PublishingChannel(this);
                channels.add(channel);
                return channel.proxy;
              case "abort":
                aborted = true;
                disconnect();
                return null;
              case "close":
                disconnect();
                return null;
              default: throw new UnsupportedOperationException(method.getName());
            }
          });
    }

    void disconnect() {
      open = false;
      channels.forEach(channel -> channel.open = false);
    }
  }

  private static class PublishingChannel {
    final Channel proxy;
    boolean open = true;
    int publishes;
    int confirms;
    int closes;
    Confirmation confirmation = () -> { };

    PublishingChannel(PublishingConnection connection) {
      proxy = (Channel) Proxy.newProxyInstance(
          Channel.class.getClassLoader(), new Class<?>[]{Channel.class}, (instance, method, args) -> {
            switch (method.getName()) {
              case "isOpen": return open && connection.open;
              case "getConnection": return connection.proxy;
              case "confirmSelect":
              case "queueDeclare":
              case "addReturnListener": return null;
              case "basicPublish":
                publishes++;
                return null;
              case "waitForConfirmsOrDie":
                confirms++;
                confirmation.await();
                return null;
              case "close":
                closes++;
                open = false;
                return null;
              default: throw new UnsupportedOperationException(method.getName());
            }
          });
    }
  }

  private final List<PublishingConnection> connections = new CopyOnWriteArrayList<>();
  private final BatchDeliveryPublisher publisher = new BatchDeliveryPublisher();
  private final Delivery delivery = new Delivery(new Envelope(1, false, "", "queue.TestService"),
      new AMQP.BasicProperties.Builder().build(), new byte[]{1, 2, 3});
  private BatchEmitter.ServiceEmitter emitter;

  @BeforeEach
  void setUp() {
    BatchEmitter batchEmitter = new BatchEmitter();
    batchEmitter.rabbitMQClient = (RabbitMQClient) Proxy.newProxyInstance(
        RabbitMQClient.class.getClassLoader(), new Class<?>[]{RabbitMQClient.class},
        (proxy, method, args) -> {
          if (method.getName().equals("connect")) {
            PublishingConnection connection = new PublishingConnection();
            connections.add(connection);
            return connection.proxy;
          }
          throw new UnsupportedOperationException(method.getName());
        });
    emitter = batchEmitter.forService("TestService");
    publisher.retryPolicy = new BatchRetryPolicy(7, List.of(5000L));
    publisher.useEmitter(emitter);
  }

  private void emit() {
    emitter.emit(new Message<>(), message -> delivery.getBody());
  }

  private void retry() throws Exception {
    publisher.retry("queue.TestService", delivery, 0, null);
  }

  @AfterEach
  void tearDown() {
    publisher.close();
    emitter.close();
  }

  @Test
  void opensOneLazyConnectionWithSeparateConfirmedChannels() throws Exception {
    assertTrue(connections.isEmpty());
    emit();
    retry();
    assertEquals(1, connections.size());
    List<PublishingChannel> channels = connections.get(0).channels;
    assertEquals(2, channels.size());
    assertNotSame(channels.get(0).proxy, channels.get(1).proxy);
    assertSame(channels.get(0).proxy.getConnection(), channels.get(1).proxy.getConnection());
    assertEquals(1, channels.get(0).confirms);
    assertEquals(1, channels.get(1).confirms);
  }

  @Test
  void retryCanOpenTheSharedConnectionBeforeTheFirstNormalEmit() throws Exception {
    retry();
    emit();
    assertEquals(1, connections.size());
    assertEquals(2, connections.get(0).channels.size());
  }

  @Test
  void receiverCloseOnlyClosesTheDeliveryPublishingChannel() throws Exception {
    emit();
    retry();
    PublishingConnection connection = connections.get(0);
    BatchReceiver receiver = new BatchReceiver();
    receiver.deliveryPublisher = publisher;
    receiver.close();
    assertTrue(connection.open);
    assertTrue(connection.channels.get(0).open);
    assertFalse(connection.channels.get(1).open);
    emit();
    retry();
    assertEquals(1, connections.size());
    assertEquals(3, connection.channels.size());
    assertEquals(2, connection.channels.get(0).publishes);
  }

  @Test
  void deliveryConfirmationFailureDoesNotInvalidateNormalEmits() throws Exception {
    emit();
    retry();
    PublishingConnection connection = connections.get(0);
    PublishingChannel deliveryChannel = connection.channels.get(1);
    deliveryChannel.confirmation = () -> { throw new IOException("confirmation failed"); };
    assertThrows(IOException.class, this::retry);
    assertFalse(deliveryChannel.open);
    assertTrue(connection.open);
    emit();
    retry();
    assertEquals(1, connections.size());
    assertEquals(3, connection.channels.size());
    assertEquals(2, connection.channels.get(0).publishes);
  }

  @Test
  void normalPublishFailureDoesNotInvalidateDeliveryChannel() throws Exception {
    emit();
    retry();
    PublishingConnection connection = connections.get(0);
    PublishingChannel normalChannel = connection.channels.get(0);
    normalChannel.confirmation = () -> { throw new IOException("confirmation failed"); };
    emit();
    assertFalse(normalChannel.open);
    assertTrue(connection.channels.get(1).open);
    retry();
    assertEquals(1, connections.size());
    assertEquals(3, connection.channels.size());
    assertEquals(2, connection.channels.get(1).publishes);
  }

  @Test
  void lostConnectionIsReplacedOnceAndBothPublishersReopenTheirChannels() throws Exception {
    emit();
    retry();
    PublishingConnection previous = connections.get(0);
    previous.disconnect();
    retry();
    emit();
    assertTrue(previous.aborted, "abandon automatic recovery on the superseded connection");
    assertEquals(2, connections.size());
    assertEquals(2, connections.get(1).channels.size());
    assertTrue(connections.get(1).open);
  }

  @Test
  void alreadyClosedChannelsAreExplicitlyClosedBeforeReplacement() throws Exception {
    emit();
    retry();
    PublishingConnection connection = connections.get(0);
    PublishingChannel normal = connection.channels.get(0);
    PublishingChannel delivery = connection.channels.get(1);
    normal.open = false;
    delivery.open = false;
    emit();
    retry();
    assertEquals(1, normal.closes);
    assertEquals(1, delivery.closes);
    assertEquals(1, connections.size());
    assertEquals(4, connection.channels.size());
  }

  @Test
  void emitterShutdownClosesAllChannelsAndCannotReconnect() throws Exception {
    emit();
    retry();
    PublishingConnection connection = connections.get(0);
    emitter.close();
    assertFalse(connection.open);
    assertTrue(connection.channels.stream().noneMatch(channel -> channel.open));
    assertThrows(IllegalStateException.class, this::retry);
    assertThrows(IllegalStateException.class, this::emit);
    assertEquals(1, connections.size());
  }

  @Test
  void closingAnUnusedEmitterDoesNotOpenAConnection() {
    emitter.close();
    assertThrows(IllegalStateException.class, this::emit);
    assertTrue(connections.isEmpty());
  }

  @Test
  void retryCanPublishWhileNormalEmitIsWaitingForConfirmation() throws Exception {
    emit();
    CountDownLatch waiting = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    connections.get(0).channels.get(0).confirmation = () -> {
      waiting.countDown();
      if (!release.await(10, TimeUnit.SECONDS)) {
        throw new IOException("test confirmation timed out");
      }
    };
    var executor = Executors.newFixedThreadPool(2);
    try {
      var normal = executor.submit(this::emit);
      assertTrue(waiting.await(5, TimeUnit.SECONDS));
      var retry = executor.submit(() -> { retry(); return null; });
      retry.get(5, TimeUnit.SECONDS);
      assertFalse(normal.isDone(), "normal publish should still be waiting on its own channel");
      assertEquals(1, connections.size());
      release.countDown();
      normal.get(5, TimeUnit.SECONDS);
    } finally {
      release.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }
}
