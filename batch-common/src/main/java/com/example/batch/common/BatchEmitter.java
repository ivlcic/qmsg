package com.example.batch.common;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import io.quarkiverse.rabbitmqclient.RabbitMQClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.UUID;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 29. 06. 2026.
 */
@Slf4j
@ApplicationScoped
public class BatchEmitter {
  @Inject
  @SuppressWarnings("CdiInjectionPointsInspection")
  RabbitMQClient rabbitMQClient;

  public static class ServiceEmitter implements AutoCloseable {
    private static final long CONFIRM_TIMEOUT_MS = 30_000;

    private final String serviceName;
    private final RabbitMQClient rabbitMQClient;
    private Connection connection;
    private Channel channel;

    private ServiceEmitter(String serviceName, RabbitMQClient rabbitMQClient) {
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

    private synchronized void closeQuietly() {
      try {
        if (channel != null && channel.isOpen()) {
          channel.close();
        }
      } catch (Exception e) {
        log.debug("Failed to close emitter channel for queue [{}]", queueName(), e);
      }
      try {
        if (connection != null && connection.isOpen()) {
          connection.close();
        }
      } catch (Exception e) {
        log.debug("Failed to close emitter connection for queue [{}]", queueName(), e);
      }
      channel = null;
      connection = null;
    }

    private synchronized Channel channel() throws IOException {
      if (connection != null && connection.isOpen() && channel != null && channel.isOpen()) {
        return channel;
      }
      closeQuietly();
      connection = rabbitMQClient.connect();
      channel = connection.createChannel();
      channel.confirmSelect();
      channel.queueDeclare(queueName(), true, false, false, null);
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
        closeQuietly();
        throw new IllegalStateException("Interrupted while publishing to queue " + queueName(), e);
      } catch (Exception first) {
        // The cached channel may be stale after a broker restart; reconnect and retry once.
        log.warn("Publish to queue [{}] failed, reconnecting and retrying once", queueName(), first);
        closeQuietly();
        try {
          publish(body);
        } catch (IOException e) {
          closeQuietly();
          throw new UncheckedIOException(e);
        } catch (Exception e) {
          closeQuietly();
          throw new IllegalStateException("Failed to publish message to queue " + queueName(), e);
        }
      }
    }

    public <P> Message<P> emit(String action, P payload, Message.Serializer serializer) throws Exception {
      Message<P> message = new Message<>();
      message.setAction(action);
      message.setPayload(payload);
      this.emit(message, serializer);
      return message;
    }

    @Override
    public synchronized void close() {
      closeQuietly();
    }
  }

  public ServiceEmitter forService(String serviceName) {
    return new ServiceEmitter(serviceName, rabbitMQClient);
  }
}
