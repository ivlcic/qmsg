package com.example.batch.common.message.rabbitmq;

import com.example.batch.common.BatchServiceException;
import com.example.batch.common.message.Emitter;
import com.example.batch.common.message.Message;
import com.example.batch.common.message.Receiver;
import com.rabbitmq.client.*;
import io.quarkiverse.rabbitmqclient.RabbitMQClient;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 29. 06. 2026.
 */
@Slf4j
@Dependent
public class RabbitMQReceiver implements Receiver {
  private static final long START_RETRY_DELAY_SECONDS = 5;

  static final class RetryHeaders {
    static final String COUNT_HEADER = "x-batch-retry-count";
    static final String TOTAL_DEFERRED_HEADER = "x-batch-total-deferred-ms";
    static final String LAST_DEFERRED_HEADER = "x-batch-last-deferred-ms";
    static final String DELIVERY_TIME_HEADER = "x-opt-delivery-time";

    private RetryHeaders() {
    }

    private static long header(Map<String, Object> headers, String key) {
      Object value = headers == null ? null : headers.get(key);
      if (value == null) {
        return 0;
      }
      if (!(value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long)) {
        throw new IllegalArgumentException("Retry header [" + key + "] must be an integer");
      }
      long number = ((Number) value).longValue();
      if (number < 0) {
        throw new IllegalArgumentException("Retry header [" + key + "] must not be negative");
      }
      return number;
    }

    static Message.RetryMetadata read(Map<String, Object> headers) {
      return new Message.RetryMetadata(
          Math.toIntExact(header(headers, COUNT_HEADER)),
          header(headers, TOTAL_DEFERRED_HEADER),
          header(headers, LAST_DEFERRED_HEADER)
      );
    }

    static void write(Message.RetryMetadata metadata, Map<String, Object> headers) {
      headers.put(COUNT_HEADER, metadata.retryCount());
      headers.put(TOTAL_DEFERRED_HEADER, metadata.totalDeferredMs());
      headers.put(LAST_DEFERRED_HEADER, metadata.lastDeferredMs());
    }
  }

  @Inject
  @SuppressWarnings("CdiInjectionPointsInspection")
  RabbitMQClient rabbitMQClient;

  @Inject
  RabbitMQDeliveryApprover deliveryApprover;

  private Connection connection;
  private Channel channel;
  private Controller controller;
  private ScheduledExecutorService retryExecutor;
  private ScheduledFuture<?> retryTask;


  private boolean shouldIgnoreShutdown(ShutdownSignalException cause) {
    return cause != null && cause.isInitiatedByApplication();
  }

  private void retryStart() {
    synchronized (this) {
      retryTask = null;
      if (controller.isRetryForbidden()) {
        return;
      }
    }
    controller.attemptStart();
  }

  private synchronized void unavailable(String reason) {
    if (controller.isRetryForbidden()) {
      return;
    }
    controller.retryPending();
    log.warn("RabbitMQ receiver for queue [{}] is unavailable: [{}]", controller.queueName(), reason);
    // Automatic recovery only covers connection-level failures; channel-level errors and abandoned
    // recovery leave the consumer dead, so a fallback retry is always scheduled. If automatic
    // recovery wins the race, recovered() cancels it and attemptStart() skips a live consumer.
    scheduleStartRetry();
  }

  private synchronized void recovered() {
    if (controller.isRetryForbidden()) {
      return;
    }
    cancelStartRetry();
    controller.retrySuccess();
    log.info("RabbitMQ receiver for queue [{}] recovered", controller.queueName());
  }

  private synchronized void consumerCancelled(String consumerTag) {
    controller.clearConsumerTag();
    if (controller.isRetryForbidden()) {
      return;
    }
    controller.retryPending();
    log.warn("Consumer [{}] for queue [{}] was cancelled by the broker", consumerTag, controller.queueName());
    scheduleStartRetry();
  }

  private void registerRecoveryListener(Recoverable recoverable) {
    recoverable.addRecoveryListener(new RecoveryListener() {
      @Override
      public void handleRecoveryStarted(Recoverable recoverable) {
        unavailable("connection recovery started");
      }

      @Override
      public void handleRecovery(Recoverable recoverable) {
        recovered();
      }
    });
  }

  private void registerListeners() {
    connection.addShutdownListener(cause -> {
      if (shouldIgnoreShutdown(cause)) {
        return;
      }
      unavailable(cause.getMessage());
    });
    channel.addShutdownListener(cause -> {
      if (shouldIgnoreShutdown(cause)) {
        return;
      }
      unavailable(cause.getMessage());
    });
    if (connection instanceof Recoverable recoverableConnection) {
      registerRecoveryListener(recoverableConnection);
    }
    if (channel instanceof Recoverable recoverableChannel) {
      registerRecoveryListener(recoverableChannel);
    }
  }

  private <M extends Message<P>, P> void handleDelivery(
      Channel deliveryChannel, Delivery delivery, Message.Reader<M, P> reader, Message.Processor<M, P> processor) {
    long deliveryTag = delivery.getEnvelope().getDeliveryTag();
    Message.Result result;
    try {
      M msg = reader.read(delivery.getBody());
      if (msg == null) {
        result = Message.Result.rejected(new BatchServiceException("Message reader returned no message"));
      } else {
        msg.setRetryMetadata(RetryHeaders.read(delivery.getProperties().getHeaders()));
        result = processor.process(msg);
        if (result == null) {
          result = Message.Result.rejected(new BatchServiceException("Message processor returned no result"));
        }
      }
    } catch (Exception e) {
      log.error("Message delivery failed for queue [{}]", controller.queueName(), e);
      result = Message.Result.rejected(e);
    }

    try {
      switch (result.disposition()) {
        case ACK -> deliveryChannel.basicAck(deliveryTag, false);
        case RETRY -> {
          deliveryApprover.retry(controller.queueName(), delivery, controller.retryPolicy(),
              result.deferMs(), result.cause());
          deliveryChannel.basicAck(deliveryTag, false);
        }
        case REJECT -> {
          log.warn("Dead-lettering delivery [{}] from queue [{}]", deliveryTag, controller.queueName());
          deliveryApprover.deadLetter(controller.queueName(), delivery, result.cause());
          deliveryChannel.basicAck(deliveryTag, false);
        }
      }
    } catch (Exception e) {
      log.warn("Failed to settle delivery [{}] for queue [{}]; retaining it for recovery",
          deliveryTag, controller.queueName(), e);
      if (channel == deliveryChannel && !controller.isRetryForbidden()) {
        controller.clearConsumerTag();
        controller.retryPending();
        close();
        scheduleStartRetry();
      }
    }
  }

  @Override
  public synchronized boolean isOpen() {
    return channel != null && channel.isOpen() && connection != null && connection.isOpen();
  }

  @Override
  public synchronized void scheduleStartRetry() {
    if (controller.isRetryForbidden()) {
      return;
    }
    if (retryTask != null && !retryTask.isDone()) {
      return;
    }
    if (retryExecutor == null || retryExecutor.isShutdown()) {
      retryExecutor = Executors.newSingleThreadScheduledExecutor(task -> {
        Thread thread = new Thread(task, controller.queueName() + "-rabbitmq-start-retry");
        thread.setDaemon(true);
        return thread;
      });
    }
    retryTask = retryExecutor.schedule(this::retryStart, START_RETRY_DELAY_SECONDS, TimeUnit.SECONDS);
  }

  @Override
  public synchronized void cancelStartRetry() {
    if (retryTask != null) {
      retryTask.cancel(false);
      retryTask = null;
    }
  }

  @Override
  public long retryDelaySeconds() {
    return START_RETRY_DELAY_SECONDS;
  }

  @Override
  public synchronized void shutdownRetrier() {
    cancelStartRetry();
    if (retryExecutor != null) {
      retryExecutor.shutdownNow();
      retryExecutor = null;
    }
  }

  @Override
  public synchronized void close() {
    deliveryApprover.close();
    try {
      if (channel != null && channel.isOpen()) {
        channel.close();
      }
      if (connection != null && connection.isOpen()) {
        connection.close();
      }
    } catch (Exception e) {
      log.warn("Failed to close receiver for queue [{}]!", controller.queueName(), e);
    } finally {
      channel = null;
      connection = null;
    }
  }

  @Override
  public synchronized void open(Controller controller, Emitter emitter) {
    if (!(emitter instanceof RabbitMQEmitter rabbitMQEmitter)) {
      throw new IllegalArgumentException("RabbitMQ receiver requires a RabbitMQ emitter");
    }
    this.controller = controller;
    deliveryApprover.useEmitter(rabbitMQEmitter);
    if (isOpen()) {
      return;
    }

    close();
    try {
      connection = rabbitMQClient.connect();
      channel = connection.createChannel();
      channel.basicQos(1);
      RabbitMQQueues.declareWorkQueue(channel, controller.queueName());
      registerListeners();
    } catch (IOException e) {
      close();
      throw new UncheckedIOException(e);
    } catch (Exception e) {
      close();
      throw new IllegalStateException("Failed to open receiver for queue " + controller.queueName(), e);
    }
  }

  @Override
  public synchronized <M extends Message<P>, P> String consume(
      Message.Reader<M, P> reader, Message.Processor<M, P> processor) {
    if (!isOpen()) {
      throw new IllegalStateException("Reader is not open");
    }
    try {
      ConsumerShutdownSignalCallback shutdownCallback = (consumerTag, cause) -> {
        if (shouldIgnoreShutdown(cause)) {
          return;
        }
        unavailable(cause == null ? "consumer shutdown" : cause.getMessage());
      };
      String queueName = controller.queueName();
      Channel deliveryChannel = channel;
      return deliveryChannel.basicConsume(
          queueName,
          false,
          queueName + ".consumer",
          (consumerTag, delivery) -> handleDelivery(deliveryChannel, delivery, reader, processor),
          this::consumerCancelled,
          shutdownCallback
      );
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public synchronized void cancel(String consumerTag) {
    if (!isOpen() || consumerTag == null) {
      return;
    }

    try {
      channel.basicCancel(consumerTag);
    } catch (Exception e) {
      // Best effort: cancel is only called on the stop/shutdown path, which must proceed to
      // close() and shutdownRetrier() even when the channel is already broken.
      log.warn("Failed to cancel consumer [{}] for queue [{}]", consumerTag, controller.queueName(), e);
    }
  }
}
