package com.example.batch.common;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class BatchQueues {
  private static final Pattern VERSION = Pattern.compile("^(\\d+)\\.(\\d+)(?:\\.|$)");

  private BatchQueues() {
  }

  private static void requireScheduledDelivery(Connection connection) {
    Map<String, Object> properties = connection.getServerProperties();
    String version = String.valueOf(properties == null ? null : properties.get("version"));
    Matcher matcher = VERSION.matcher(version);
    if (matcher.find()) {
      int major = Integer.parseInt(matcher.group(1));
      int minor = Integer.parseInt(matcher.group(2));
      if (major > 4 || (major == 4 && minor >= 4)) {
        return;
      }
    }
    throw new IllegalStateException("Scheduled batch retries require RabbitMQ 4.4+; broker version is " + version);
  }

  static void declareWorkQueue(Channel channel, String queueName) throws IOException {
    requireScheduledDelivery(channel.getConnection());
    channel.queueDeclare(queueName, true, false, false, Map.of("x-queue-type", "quorum"));
  }
}
