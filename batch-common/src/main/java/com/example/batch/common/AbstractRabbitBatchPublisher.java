package com.example.batch.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import io.quarkiverse.rabbitmqclient.RabbitMQClient;
import jakarta.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

public abstract class AbstractRabbitBatchPublisher {
  @Inject
  RabbitMQClient rabbitMQClient;

  @Inject
  ObjectMapper objectMapper;

  protected abstract String queueName();

  public <P> Message<P> publish(String action, P payload) {
    Message<P> message = new Message<>();
    message.setAction(action);
    message.setPayload(payload);
    publish(message);
    return message;
  }

  public <P> void publish(Message<P> message) {
    try (Connection connection = rabbitMQClient.connect(); Channel channel = connection.createChannel()) {
      channel.queueDeclare(queueName(), true, false, false, null);
      byte[] body = objectMapper.writeValueAsString(message).getBytes(StandardCharsets.UTF_8);
      AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
          .contentType("application/json")
          .deliveryMode(2)
          .build();
      channel.basicPublish("", queueName(), properties, body);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to publish message to queue " + queueName(), e);
    }
  }
}
