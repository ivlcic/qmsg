package com.example.batch.common;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConsumerShutdownSignalCallback;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.client.ShutdownSignalException;
import io.quarkiverse.rabbitmqclient.RabbitMQClient;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Dependent
public class BatchReceiver implements AutoCloseable {
  private static final long START_RETRY_DELAY_SECONDS = 5;
  private static final Logger LOG = Logger.getLogger(BatchReceiver.class);

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
    LOG.warnf("RabbitMQ receiver for queue %s is unavailable: %s", controller.queueName(), reason);
    if (!(connection instanceof Recoverable && channel instanceof Recoverable)) {
      scheduleStartRetry();
    }
  }

  private synchronized void recovered() {
    if (controller.isRetryForbidden()) {
      return;
    }
    controller.retrySuccess();
    LOG.infof("RabbitMQ receiver for queue %s recovered", controller.queueName());
  }

  private synchronized void consumerCancelled(String consumerTag) {
    controller.clearConsumerTag();
    if (controller.isRetryForbidden()) {
      return;
    }
    controller.retryPending();
    LOG.warnf("Consumer %s for queue %s was cancelled by the broker", consumerTag, controller.queueName());
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
      Delivery delivery, Message.Reader<M, P> reader, Message.Processor<M, P> processor) throws IOException {
    long deliveryTag = delivery.getEnvelope().getDeliveryTag();
    boolean ack = false;
    M msg;
    try {
      msg = reader.read(delivery.getBody());
      try {
        ack = processor.process(msg);
      } catch (Exception e) {
        LOG.errorf(e, "Message processor failed for queue %s", controller.queueName());
      }
    } catch (Exception e) {
      LOG.errorf(e, "Message reader failed for queue %s", controller.queueName());
    }

    if (ack) {
      channel.basicAck(deliveryTag, false);
    } else {
      channel.basicNack(deliveryTag, false, true);
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
    if (retryExecutor != null && !retryExecutor.isShutdown()) {
      return;
    }
    retryExecutor = Executors.newSingleThreadScheduledExecutor(task -> {
      Thread thread = new Thread(task, controller.queueName() + "-rabbitmq-start-retry");
      thread.setDaemon(true);
      return thread;
    });
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
      LOG.warnf(e, "Failed to close receiver for queue %s", controller.queueName());
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
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
