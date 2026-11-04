package com.example.batch.common;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Return;
import io.quarkiverse.rabbitmqclient.RabbitMQClient;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Dependent
public class BatchDeadLetterPublisher implements AutoCloseable {
  private static final long CONFIRM_TIMEOUT_MS = 30_000;

  @Inject
  RabbitMQClient rabbitMQClient;

  private Connection connection;
  private Channel channel;
  private final AtomicReference<Return> returned = new AtomicReference<>();

  @Override
  public synchronized void close() {
    try {
      if (channel != null && channel.isOpen()) {
        channel.close();
      }
    } catch (Exception e) {
      log.debug("Failed to close dead-letter publisher channel", e);
    }
    try {
      if (connection != null && connection.isOpen()) {
        connection.close();
      }
    } catch (Exception e) {
      log.debug("Failed to close dead-letter publisher connection", e);
    }
    channel = null;
    connection = null;
  }

  private Channel channel() throws Exception {
    if (connection != null && connection.isOpen() && channel != null && channel.isOpen()) {
      return channel;
    }
    close();
    connection = rabbitMQClient.connect();
    channel = connection.createChannel();
    channel.confirmSelect();
    channel.addReturnListener(returned::set);
    return channel;
  }

  private AMQP.BasicProperties properties(String sourceQueue, Delivery delivery, Throwable cause) {
    AMQP.BasicProperties original = delivery.getProperties();
    Map<String, Object> headers = new HashMap<>();
    if (original.getHeaders() != null) {
      headers.putAll(original.getHeaders());
    }
    headers.put("x-batch-source-queue", sourceQueue);
    if (cause != null) {
      headers.put("x-batch-failure-type", cause.getClass().getName());
      if (cause.getMessage() != null) {
        headers.put("x-batch-failure-message", cause.getMessage());
      }
    }
    // A failed message must not expire in the final DLQ or impersonate its original publisher.
    return original.builder().headers(headers).deliveryMode(2).expiration(null).userId(null).build();
  }

  public synchronized void publish(String sourceQueue, Delivery delivery, Throwable cause) throws Exception {
    String destination = sourceQueue + ".dlq";
    try {
      Channel publisher = channel();
      publisher.queueDeclare(destination, true, false, false, null);
      returned.set(null);
      publisher.basicPublish("", destination, true, properties(sourceQueue, delivery, cause), delivery.getBody());
      publisher.waitForConfirmsOrDie(CONFIRM_TIMEOUT_MS);
      if (returned.get() != null) {
        throw new IllegalStateException("Dead-letter message was not routed to " + destination);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      close();
      throw e;
    } catch (Exception e) {
      close();
      throw e;
    }
  }
}
