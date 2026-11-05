package com.example.batch.common;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Return;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Dependent
public class BatchDeliveryPublisher implements AutoCloseable {
  private static final long CONFIRM_TIMEOUT_MS = 30_000;

  @Inject
  BatchRetryPolicy retryPolicy;

  private BatchEmitter.ServiceEmitter emitter;
  private Channel channel;
  private final AtomicReference<Return> returned = new AtomicReference<>();

  @Override
  public synchronized void close() {
    try {
      if (channel != null) {
        channel.close();
      }
    } catch (Exception e) {
      log.debug("Failed to close delivery publisher channel", e);
    }
    channel = null;
  }

  synchronized void useEmitter(BatchEmitter.ServiceEmitter emitter) {
    Objects.requireNonNull(emitter, "Service emitter is required");
    if (this.emitter != emitter) {
      close();
      this.emitter = emitter;
    }
  }

  private Channel channel() throws Exception {
    if (channel != null && channel.isOpen()) {
      return channel;
    }
    close();
    if (emitter == null) {
      throw new IllegalStateException("Delivery publisher has no service emitter");
    }
    channel = emitter.openChannel();
    channel.confirmSelect();
    channel.addReturnListener(returned::set);
    return channel;
  }

  private Map<String, Object> headers(String sourceQueue, Delivery delivery, Throwable cause) {
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
    return headers;
  }

  private void publish(String destination, Delivery delivery, Map<String, Object> headers, boolean workQueue)
      throws Exception {
    try {
      Channel publisher = channel();
      if (workQueue) {
        BatchQueues.declareWorkQueue(publisher, destination);
        long delayMs = ((Number) headers.get(Message.RetryMetadata.LAST_DEFERRED_HEADER)).longValue();
        headers.put(Message.DELIVERY_TIME_HEADER, Math.addExact(System.currentTimeMillis(), delayMs));
      } else {
        publisher.queueDeclare(destination, true, false, false, null);
      }
      // Republished messages must not inherit a TTL or impersonate their original publisher.
      AMQP.BasicProperties properties = delivery.getProperties().builder()
          .headers(headers).deliveryMode(2).expiration(null).userId(null).build();
      returned.set(null);
      publisher.basicPublish("", destination, true, properties, delivery.getBody());
      publisher.waitForConfirmsOrDie(CONFIRM_TIMEOUT_MS);
      if (returned.get() != null) {
        throw new IllegalStateException("Republished message was not routed to " + destination);
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

  public synchronized void deadLetter(String sourceQueue, Delivery delivery, Throwable cause) throws Exception {
    Map<String, Object> headers = headers(sourceQueue, delivery, cause);
    headers.remove(Message.DELIVERY_TIME_HEADER);
    publish(sourceQueue + ".dlq", delivery, headers, false);
  }

  public synchronized void retry(String sourceQueue, Delivery delivery, long requestedDelayMs, Throwable cause)
      throws Exception {
    try {
      Message.RetryMetadata current = Message.RetryMetadata.fromHeaders(delivery.getProperties().getHeaders());
      if (current.retryCount() >= retryPolicy.getMaxRetries()) {
        log.warn("Retry limit [{}] reached for queue [{}]", retryPolicy.getMaxRetries(), sourceQueue);
        deadLetter(sourceQueue, delivery, cause);
        return;
      }
      long delayMs = retryPolicy.delayMs(Math.incrementExact(current.retryCount()), requestedDelayMs);
      Message.RetryMetadata next = current.next(delayMs);
      Map<String, Object> headers = headers(sourceQueue, delivery, cause);
      next.writeHeaders(headers);
      publish(sourceQueue, delivery, headers, true);
      log.info("Scheduled retry [{}] for queue [{}] in [{}]ms; total deferred [{}]ms",
          next.retryCount(), sourceQueue, delayMs, next.totalDeferredMs());
    } catch (IllegalArgumentException | ArithmeticException e) {
      log.error("Invalid retry metadata or delay for queue [{}]", sourceQueue, e);
      deadLetter(sourceQueue, delivery, e);
    }
  }
}
