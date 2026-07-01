package com.example.batch.common;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import io.quarkiverse.rabbitmqclient.RabbitMQClient;

import java.io.IOException;
import java.io.UncheckedIOException;

public class BatchEmitter {
  private final String serviceName;
  private final RabbitMQClient rabbitMQClient;

  BatchEmitter(String serviceName, RabbitMQClient rabbitMQClient) {
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

  private Message.Writer emitter(Channel channel) {
    return body -> channel.basicPublish("", queueName(), properties(), body);
  }

  public <P> void emit(Message<P> message, Message.Serializer serializer) {
    try (Connection connection = rabbitMQClient.connect(); Channel channel = connection.createChannel()) {
      channel.queueDeclare(queueName(), true, false, false, null);
      emitter(channel).write(serializer.serialize(message));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to publish message to queue " + queueName(), e);
    }
  }

  public <P> Message<P> emit(String action, P payload, Message.Serializer serializer) throws Exception {
    Message<P> message = new Message<>();
    message.setAction(action);
    message.setPayload(payload);
    this.emit(message, serializer);
    return message;
  }
}
