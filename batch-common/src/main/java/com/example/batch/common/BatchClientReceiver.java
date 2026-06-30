package com.example.batch.common;

import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Delivery;
import io.quarkiverse.rabbitmqclient.RabbitMQClient;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;

@Dependent
public class BatchClientReceiver implements AutoCloseable {
  private static final Logger LOG = Logger.getLogger(BatchClientReceiver.class);

  @Inject
  @SuppressWarnings("CdiInjectionPointsInspection")
  RabbitMQClient rabbitMQClient;

  private Connection connection;
  private Channel channel;
  private String queueName;

  private boolean isOpenFor(String queueName) {
    return isOpen() && queueName.equals(this.queueName);
  }

  private boolean isOpen() {
    return channel != null && channel.isOpen() && connection != null && connection.isOpen();
  }

  private void ensureOpen() {
    if (!isOpen()) {
      throw new IllegalStateException("Receiver is not open");
    }
  }

  private void handleDelivery(Delivery delivery, MessageHandler messageHandler) throws IOException {
    long deliveryTag = delivery.getEnvelope().getDeliveryTag();
    boolean ack = false;
    try {
      ack = messageHandler.handle(delivery.getBody());
    } catch (Exception e) {
      LOG.errorf(e, "Message handler failed for queue %s", queueName);
    }

    if (ack) {
      channel.basicAck(deliveryTag, false);
    } else {
      channel.basicNack(deliveryTag, false, true);
    }
  }

  public synchronized void open(String queueName) {
    if (isOpenFor(queueName)) {
      return;
    }

    close();
    try {
      connection = rabbitMQClient.connect();
      channel = connection.createChannel();
      channel.basicQos(1);
      channel.queueDeclare(queueName, true, false, false, null);
      this.queueName = queueName;
    } catch (IOException e) {
      close();
      throw new UncheckedIOException(e);
    } catch (Exception e) {
      close();
      throw new IllegalStateException("Failed to open receiver for queue " + queueName, e);
    }
  }

  public synchronized String consume(MessageHandler messageHandler, CancelCallback cancelCallback) {
    ensureOpen();
    try {
      return channel.basicConsume(queueName, false,
          (consumerTag, delivery) -> handleDelivery(delivery, messageHandler),
          cancelCallback);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public synchronized void cancel(String consumerTag) {
    if (!isOpen() || consumerTag == null) {
      return;
    }

    try {
      channel.basicCancel(consumerTag);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public synchronized void close() {
    try {
      if (channel != null && channel.isOpen()) {
        channel.close();
      }
      if (connection != null && connection.isOpen()) {
        connection.close();
      }
    } catch (Exception e) {
      LOG.warnf(e, "Failed to close receiver for queue %s", queueName);
    } finally {
      channel = null;
      connection = null;
      queueName = null;
    }
  }

  @FunctionalInterface
  public interface MessageHandler {
    boolean handle(byte[] body) throws Exception;
  }
}
