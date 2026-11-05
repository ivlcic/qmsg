package com.example.batch.common.message.rabbitmq;

import com.example.batch.common.message.Emitter;
import com.example.batch.common.message.Message;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import io.quarkiverse.rabbitmqclient.RabbitMQClient;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.UUID;

/**
 * @author Nikola Ivacic <nikola.ivacic@dropchop.com> on 05. 11. 2026.
 */
@Slf4j
public class RabbitMQEmitter implements Emitter {
  private static final long CONFIRM_TIMEOUT_MS = 30_000;

  private final String serviceName;
  private final RabbitMQClient rabbitMQClient;
  private final Object connectionLock = new Object();
  private Connection connection;
  private Channel channel;
  private boolean closed;

  RabbitMQEmitter(String serviceName, RabbitMQClient rabbitMQClient) {
    if (serviceName == null || serviceName.isBlank()) {
      throw new IllegalArgumentException("Service name is required");
    }
    if (rabbitMQClient == null) {
      throw new IllegalArgumentException("RabbitMQ client is required");
    }
    this.serviceName = serviceName;
    this.rabbitMQClient = rabbitMQClient;
  }

  protected String queueName() {
    return "queue." + serviceName;
  }

  private AMQP.BasicProperties properties() {
    return new AMQP.BasicProperties.Builder()
        .contentType("application/json")
        .deliveryMode(2)
        .build();
  }

  private void closeChannel() {
    try {
      if (channel != null) {
        // Also unregister already-closed channels from automatic recovery.
        channel.close();
      }
    } catch (Exception e) {
      log.debug("Failed to close emitter channel for queue [{}]", queueName(), e);
    }
    channel = null;
  }

  // Borrowers own their channels, but only this emitter owns the publishing connection.
  // Do not hold the emit lock while creating a retry/DLQ channel.
  Channel openChannel() throws IOException {
    synchronized (connectionLock) {
      if (closed) {
        throw new IllegalStateException("Emitter is closed for queue " + queueName());
      }
      if (connection == null || !connection.isOpen()) {
        if (connection != null) {
          connection.abort();
        }
        connection = rabbitMQClient.connect();
      }
      Channel publisher = connection.createChannel();
      if (publisher == null) {
        throw new IOException("No publishing channel available for queue " + queueName());
      }
      return publisher;
    }
  }

  private synchronized Channel channel() throws IOException {
    if (channel != null && channel.isOpen()) {
      return channel;
    }
    closeChannel();
    channel = openChannel();
    channel.confirmSelect();
    RabbitMQQueues.declareWorkQueue(channel, queueName());
    return channel;
  }

  private Message.Writer writer(Channel channel) {
    return body -> {
      channel.basicPublish("", queueName(), properties(), body);
      channel.waitForConfirmsOrDie(CONFIRM_TIMEOUT_MS);
    };
  }

  private synchronized void publish(byte[] body) throws Exception {
    writer(channel()).write(body);
  }

  @Override
  public synchronized <P> void emit(Message<P> message, Message.Serializer serializer) {
    if (message.getId() == null || message.getId().isBlank()) {
      message.setId(UUID.randomUUID().toString());
    }
    byte[] body;
    try {
      body = serializer.serialize(message);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to serialize message for queue " + queueName(), e);
    }
    try {
      publish(body);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      closeChannel();
      throw new IllegalStateException("Interrupted while publishing to queue " + queueName(), e);
    } catch (Exception first) {
      // Replace only this channel; a live connection can still serve retry/DLQ publishes.
      log.warn("Publish to queue [{}] failed, reopening the channel and retrying once", queueName(), first);
      closeChannel();
      try {
        publish(body);
      } catch (IOException e) {
        closeChannel();
        throw new UncheckedIOException(e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        closeChannel();
        throw new IllegalStateException("Interrupted while publishing to queue " + queueName(), e);
      } catch (Exception e) {
        closeChannel();
        throw new IllegalStateException("Failed to publish message to queue " + queueName(), e);
      }
    }
  }

  @Override
  public <P> Message<P> emit(String action, P payload, Message.Serializer serializer) {
    Message<P> message = new Message<>();
    message.setAction(action);
    message.setPayload(payload);
    this.emit(message, serializer);
    return message;
  }

  @Override
  public synchronized void close() {
    closeChannel();
    synchronized (connectionLock) {
      closed = true;
      try {
        if (connection != null) {
          connection.close();
        }
      } catch (Exception e) {
        log.debug("Failed to close emitter connection for queue [{}]", queueName(), e);
      } finally {
        connection = null;
      }
    }
  }
}
