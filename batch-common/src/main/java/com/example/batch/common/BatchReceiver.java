package com.example.batch.common;

import com.rabbitmq.client.*;
import io.quarkiverse.rabbitmqclient.RabbitMQClient;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 29. 06. 2026.
 */
@Slf4j
@Dependent
public class BatchReceiver implements AutoCloseable {
  private static final long START_RETRY_DELAY_SECONDS = 5;

  public interface Controller {
    String queueName();

    boolean isRetryForbidden();

    void clearConsumerTag();

    void retryPending();

    void retrySuccess();

    void attemptStart();
  }

  @Inject
  @SuppressWarnings("CdiInjectionPointsInspection")
  RabbitMQClient rabbitMQClient;

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
      Delivery delivery, Message.Reader<M, P> reader, Message.Processor<M, P> processor) {
    long deliveryTag = delivery.getEnvelope().getDeliveryTag();
    boolean ack = false;
    boolean requeue = true;
    M msg = null;
    try {
      msg = reader.read(delivery.getBody());
    } catch (Exception e) {
      log.error("Message reader failed for queue [{}]", controller.queueName(), e);
    }
    if (msg == null) {
      // An unreadable message can never succeed; requeueing it would redeliver it immediately
      // (basicQos(1)) and spin the consumer forever while blocking the rest of the queue.
      log.error("Discarding unreadable message from queue [{}]", controller.queueName());
      requeue = false;
    } else {
      try {
        ack = processor.process(msg);
      } catch (Exception e) {
        log.error("Message processor failed for queue [{}]", controller.queueName(), e);
      }
    }

    try {
      if (ack) {
        channel.basicAck(deliveryTag, false);
      } else {
        channel.basicNack(deliveryTag, false, requeue);
      }
    } catch (Exception e) {
      // The channel died between delivery and (n)ack; the unacked message will be redelivered
      // after recovery, and the shutdown listeners drive the reconnect.
      log.warn("Failed to ack/nack delivery [{}] for queue [{}]", deliveryTag, controller.queueName(), e);
    }
  }

  public synchronized boolean isOpen() {
    return channel != null && channel.isOpen() && connection != null && connection.isOpen();
  }

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

  public synchronized void cancelStartRetry() {
    if (retryTask != null) {
      retryTask.cancel(false);
      retryTask = null;
    }
  }

  public long retryDelaySeconds() {
    return START_RETRY_DELAY_SECONDS;
  }

  public synchronized void shutdownRetrier() {
    cancelStartRetry();
    if (retryExecutor != null) {
      retryExecutor.shutdownNow();
      retryExecutor = null;
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
      log.warn("Failed to close receiver for queue [{}]!", controller.queueName(), e);
    } finally {
      channel = null;
      connection = null;
    }
  }

  public synchronized void open(Controller controller) {
    this.controller = controller;
    if (isOpen()) {
      return;
    }

    close();
    try {
      connection = rabbitMQClient.connect();
      channel = connection.createChannel();
      channel.basicQos(1);
      channel.queueDeclare(controller.queueName(), true, false, false, null);
      registerListeners();
    } catch (IOException e) {
      close();
      throw new UncheckedIOException(e);
    } catch (Exception e) {
      close();
      throw new IllegalStateException("Failed to open receiver for queue " + controller.queueName(), e);
    }
  }

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
      return channel.basicConsume(
          queueName,
          false,
          queueName + ".consumer",
          (consumerTag, delivery) -> handleDelivery(delivery, reader, processor),
          this::consumerCancelled,
          shutdownCallback
      );
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
    } catch (Exception e) {
      // Best effort: cancel is only called on the stop/shutdown path, which must proceed to
      // close() and shutdownRetrier() even when the channel is already broken.
      log.warn("Failed to cancel consumer [{}] for queue [{}]", consumerTag, controller.queueName(), e);
    }
  }
}
