package com.example.batch.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.arc.All;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractBatchService implements BatchService {
  private static final Logger LOG = Logger.getLogger(AbstractBatchService.class);

  @Inject
  BatchReceiver receiver;

  @Inject
  ObjectMapper objectMapper;

  @Inject
  BatchMetrics batchMetrics;

  @Inject
  BatchEmitter batchEmitter;

  @Inject
  @All
  @SuppressWarnings("CdiInjectionPointsInspection")
  List<BatchStep<?>> availableSteps;

  private final AtomicReference<BatchServiceState> state = new AtomicReference<>(BatchServiceState.initializing);
  private final Actions actions;
  private BatchMetrics.ServiceMetrics metrics;
  private BatchEmitter.ServiceEmitter emitter;
  private String consumerTag;

  protected AbstractBatchService(Actions actions) {
    this.actions = actions;
  }

  @Override
  public String getName() {
    return getClass().getSimpleName();
  }

  protected String queueName() {
    return "queue." + getName();
  }

  @Override
  public BatchStatus status() {
    return new BatchStatus(queueName(), state.get(), consumerTag);
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
    for (Class<? extends BatchStep<P>> stepType : action.stepTypes()) {
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

  public <P> boolean execute(BatchContext<P> context) throws Exception {
    String actionName = normalizeAction(context.action());
    Action<?> targetAction = actions.get(actionName);
    if (targetAction == null || targetAction.isEmpty()) {
      metrics.recordExecutionError(actionName);
      throw new RuntimeException(
          "No steps found for action [" + actionName + "] in [" + getName() + "] service!"
      );
    }
    @SuppressWarnings("unchecked")
    Action<P> a = (Action<P>) targetAction;
    int stepIndex = 0;
    for (BatchStep<P> step : a) {
      String stepName = a.stepTypes().get(stepIndex).getSimpleName();
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
        LOG.errorf(e, "Failed to process message from queue %s", queueName());
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

  @Override
  public synchronized BatchStatus start() {
    if (state.get() == BatchServiceState.started || state.get() == BatchServiceState.starting) {
      return status();
    }

    state.set(BatchServiceState.starting);
    receiver.open(queueName());
    try {
      consumerTag = receiver.consume(getReader(), getProcessor(), tag -> {
        state.set(BatchServiceState.stopped);
        LOG.infof("Consumer for queue %s was cancelled by the broker", queueName());
      });
      state.set(BatchServiceState.started);
      LOG.infof("Started consuming queue %s with consumer tag %s", queueName(), consumerTag);
    } catch (RuntimeException e) {
      state.set(BatchServiceState.stopped);
      throw e;
    }
    return status();
  }

  @Override
  public synchronized BatchStatus stop() {
    if (state.get() != BatchServiceState.started) {
      return status();
    }

    state.set(BatchServiceState.stopping);
    receiver.cancel(consumerTag);
    state.set(BatchServiceState.stopped);
    LOG.infof("Stopped consuming queue %s", queueName());
    return status();
  }

  void onApplicationStart(@Observes StartupEvent event) {
    receiver.open(queueName());
    start();
  }

  void onApplicationShutdown(@Observes ShutdownEvent event) {
    stop();
    receiver.close();
  }
}
