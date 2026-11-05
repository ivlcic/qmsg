package com.example.batch.common.message;

import com.example.batch.common.RetryPolicy;

/**
 * @author Nikola Ivacic <nikola.ivacic@dropchop.com> on 05. 11. 2026.
 */
public interface Receiver extends AutoCloseable {
  interface Controller {
    String queueName();

    RetryPolicy retryPolicy();

    boolean isRetryForbidden();

    void clearConsumerTag();

    void retryPending();

    void retrySuccess();

    void attemptStart();
  }

  boolean isOpen();

  void scheduleStartRetry();

  void cancelStartRetry();

  long retryDelaySeconds();

  void shutdownRetrier();

  @Override
  void close();

  void open(Controller controller, Emitter emitter);

  <M extends Message<P>, P> String consume(Message.Reader<M, P> reader, Message.Processor<M, P> processor);

  void cancel(String consumerTag);
}
