package com.example.batch.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.arc.All;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.context.control.RequestContextController;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 29. 06. 2026.
 */
@Slf4j
public abstract class AbstractBatchService implements BatchService {
  @Inject
  BatchReceiver receiver;

  @Inject
  ObjectMapper objectMapper;

  @Inject
  BatchMetrics batchMetrics;

  @Inject
  BatchEmitter batchEmitter;

  @Inject
  RequestContextController requestContextController;

  @Inject
  @All
  @SuppressWarnings("CdiInjectionPointsInspection")
  List<BatchStep<?>> availableSteps;

  private final AtomicReference<BatchServiceState> state = new AtomicReference<>(BatchServiceState.initializing);
  private final Actions actions;
  private BatchMetrics.ServiceMetrics metrics;
  private BatchEmitter.ServiceEmitter emitter;
  private volatile String consumerTag;

  private class ReceiverRetryController implements BatchReceiver.Controller {

    @Override
    public String queueName() {
      return "queue." + getName();
    }

    @Override
    public boolean isRetryForbidden() {
      BatchServiceState current = state.get();
      return current != BatchServiceState.starting && current != BatchServiceState.started;
    }

    @Override
    public void clearConsumerTag() {
      consumerTag = null;
    }

    @Override
    public void retryPending() {
      state.set(BatchServiceState.starting);
    }

    @Override
    public void retrySuccess() {
      state.set(BatchServiceState.started);
    }

    @Override
    public void attemptStart() {
      AbstractBatchService.this.attemptStart();
    }
  }

  protected AbstractBatchService(Actions actions) {
    this.actions = actions;
  }

  @Override
  public String getName() {
    return getClass().getSimpleName();
  }

  @Override
  public BatchStatus status() {
    return new BatchStatus(getName(), state.get(), consumerTag);
  }

  private static String normalizeAction(String action) {
    if (action == null || action.isBlank()) {
      return DEFAULT_ACTION;
    }
    return action;
  }

  private <S extends BatchStep<P>, P> S resolveStep(Class<S> type) {
    for (BatchStep<?> step : availableSteps) {
      if (type.isInstance(step)) {
        //noinspection unchecked
        return (S) step;
      }
    }
    throw new IllegalStateException("No batch step bean found for " + type.getName());
  }

  private <P> void initializeAction(Action<P> action) {
    for (Class<? extends BatchStep<P>> stepType : action.getStepTypes()) {
      action.addStep(resolveStep(stepType));
    }
  }

  @PostConstruct
  void initializeSteps() {
    metrics = batchMetrics.forService(getName());
    emitter = batchEmitter.forService(getName());
    for (Action<?> action : actions.values()) {
      initializeAction(action);
    }
    state.set(BatchServiceState.initialized);
  }

  protected <X> Message.Deserializer<X> getDeserializer() {
    return new DefaultDeserializer<>(objectMapper);
  }

  protected Message.Serializer getSerializer() {
    return new DefaultSerializer(objectMapper);
  }

  protected BatchEmitter.ServiceEmitter getEmitter() {
    return emitter;
  }

  private <P> boolean executeSteps(Action<P> action, String actionName, BatchContext<P> context) throws Exception {
    int stepIndex = 0;
    for (BatchStep<P> step : action) {
      String stepName = action.getStepTypes().get(stepIndex).getSimpleName();
      try {
        step.execute(context);
        metrics.recordStep(actionName, stepName);
      } catch (Exception e) {
        metrics.recordExecutionError(actionName);
        metrics.recordStepError(actionName, stepName);
        throw e;
      }
      stepIndex++;
    }
    metrics.recordExecution(actionName);
    return true;
  }

  private <P> boolean executeInRequestContext(Action<P> action, String actionName, BatchContext<P> context) throws Exception {
    boolean activated = requestContextController.activate();
    try {
      return executeSteps(action, actionName, context);
    } finally {
      if (activated) {
        requestContextController.deactivate();
      }
    }
  }

  public <P> boolean execute(BatchContext<P> context) throws Exception {
    String actionName = normalizeAction(context.getAction());
    Action<?> targetAction = actions.get(actionName);
    if (targetAction == null || targetAction.isEmpty()) {
      metrics.recordExecutionError(actionName);
      throw new RuntimeException(
          "No steps found for action [" + actionName + "] in [" + getName() + "] service!"
      );
    }
    @SuppressWarnings("unchecked")
    Action<P> a = (Action<P>) targetAction;
    return executeInRequestContext(a, actionName, context);
  }

  public <P> boolean execute(String requestedAction, P payload) throws Exception {
    BatchContext<P> context = new BatchContext<>(requestedAction, payload);
    return execute(context);
  }

  protected <M extends Message<P>, P> Message.Processor<M, P> getProcessor() {
    return message -> execute(new BatchContext<>(message.getAction(), message.getPayload()));
  }

  protected <M extends Message<P>, P> Message.Reader<M, P> getReader() {
    return body -> {
      try {
        Message.Deserializer<Message<P>> nodeDeserializer = getDeserializer();
        //noinspection unchecked
        return (M)nodeDeserializer.deserialize(body);
      } catch (Exception e) {
        log.error("Failed to process message from queue for service [{}]!", getName(), e);
        return null;
      }
    };
  }

  public <P> Message<P> emit(String action, P payload) throws Exception {
    return getEmitter().emit(action, payload, getSerializer());
  }

  public <P> Message<P> emit(String action, P payload, Message.Serializer serializer) throws Exception {
    return getEmitter().emit(action, payload, serializer);
  }

  private synchronized BatchStatus attemptStart() {
    BatchReceiver.Controller controller = new ReceiverRetryController();
    try {
      receiver.open(controller);
      consumerTag = receiver.consume(getReader(), getProcessor());
      controller.retrySuccess();
      log.info("Started consuming queue [{}] with consumer tag [{}]", controller.queueName(), consumerTag);
    } catch (RuntimeException e) {
      consumerTag = null;
      controller.retryPending();
      receiver.close();
      log.warn(
          "Failed to start RabbitMQ consumer for queue [{}]; retrying in [{}] seconds",
          controller.queueName(), receiver.retryDelaySeconds(), e
      );
      receiver.scheduleStartRetry();
    }
    return status();
  }

  @Override
  public synchronized BatchStatus start() {
    if (state.get() == BatchServiceState.started || state.get() == BatchServiceState.starting) {
      return status();
    }

    state.set(BatchServiceState.starting);
    return attemptStart();
  }

  @Override
  public synchronized BatchStatus stop() {
    receiver.cancelStartRetry();
    state.set(BatchServiceState.stopping);
    receiver.cancel(consumerTag);
    consumerTag = null;
    receiver.close();
    state.set(BatchServiceState.stopped);
    log.info("Stopped consuming queue for service [{}]", getName());
    return status();
  }

  void onApplicationStart(@Observes StartupEvent event) {
    start();
  }

  void onApplicationShutdown(@Observes ShutdownEvent event) {
    stop();
    receiver.shutdownRetrier();
  }
}
