package com.example.batch.common.message.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Nikola Ivacic <nikola.ivacic@dropchop.com> on 05. 11. 2026.
 */
class RabbitMQQueuesTest {
  private final AtomicReference<Object[]> declaration = new AtomicReference<>();

  private Channel channel(String version) {
    Connection connection = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
        new Class<?>[]{Connection.class}, (proxy, method, args) -> {
          if (method.getName().equals("getServerProperties")) {
            return version == null ? Map.of() : Map.of("version", version);
          }
          throw new UnsupportedOperationException(method.getName());
        });
    return (Channel) Proxy.newProxyInstance(Channel.class.getClassLoader(), new Class<?>[]{Channel.class},
        (proxy, method, args) -> {
          if (method.getName().equals("getConnection")) {
            return connection;
          }
          if (method.getName().equals("queueDeclare")) {
            declaration.set(args);
            return null;
          }
          throw new UnsupportedOperationException(method.getName());
        });
  }

  @Test
  void workQueuesAreDeclaredDurableAndQuorumOnSupportedBroker() throws Exception {
    RabbitMQQueues.declareWorkQueue(channel("4.4.0"), "queue.TestService");
    assertArrayEquals(new Object[]{"queue.TestService", true, false, false, Map.of("x-queue-type", "quorum")},
        declaration.get());
  }

  @Test
  void unsupportedOrUnknownBrokerVersionFailsBeforeDeclaration() {
    for (String version : new String[]{"3.13.7", "4.3.9", "unknown", null}) {
      assertThrows(IllegalStateException.class, () -> RabbitMQQueues.declareWorkQueue(channel(version), "queue.TestService"));
      assertNull(declaration.get());
    }
  }
}
