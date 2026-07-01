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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractBatchService implements BatchService {
  private static final Logger LOG = Logger.getLogger(AbstractBatchService.class);

  @Inject
  BatchReceiver receiver;

  @Inject
  ObjectMapper objectMapper;

  @Inject
  @All
  @SuppressWarnings("CdiInjectionPointsInspection")
  List<BatchStep<?>> availableSteps;

  private final AtomicBoolean consuming = new AtomicBoolean(false);
  private final Actions actions;
  private String consumerTag;

  protected AbstractBatchService(Actions actions) {
    this.actions = actions;
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
    for (Action<?> action : actions.values()) {
      initializeAction(action);
    }
  }

  private static String normalizeAction(String action) {
    if (action == null || action.isBlank()) {
      return DEFAULT_ACTION;
    }
    return action;
  }

  protected <X> Message.Deserializer<X> getDeserializer() {
    return new DefaultDeserializer<>(objectMapper);
  }

  protected Message.Serializer getSerializer() {
    return new DefaultSerializer(objectMapper);
  }

  protected BatchEmitter getEmitter() {
    return new BatchEmitter(getName(), receiver.rabbitMQClient);
  }

  protected <M extends Message<P>, P> Message.Processor<M, P> getProcessor() {
    return message -> execute(message.getAction(), message.getPayload(), Optional.empty());
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

  @Override
  public String getName() {
    return getClass().getSimpleName();
  }

  protected String queueName() {
    return "queue." + getName();
  }

  public <P> Message<P> emit(String action, P payload) throws Exception {
    return getEmitter().emit(action, payload, getSerializer());
  }

  public <P> Message<P> emit(String action, P payload, Message.Serializer serializer) throws Exception {
    return getEmitter().emit(action, payload, serializer);
  }

  void onApplicationStart(@Observes StartupEvent event) {
    receiver.open(queueName());
    start();
  }

  @Override
  public synchronized BatchStatus start() {
    if (consuming.get()) {
      return status();
    }

    receiver.open(queueName());
    consumerTag = receiver.consume(getReader(), getProcessor(), tag -> {
      consuming.set(false);
      LOG.infof("Consumer for queue %s was cancelled by the broker", queueName());
    });
    consuming.set(true);
    LOG.infof("Started consuming queue %s with consumer tag %s", queueName(), consumerTag);
    return status();
  }

  public <P> boolean execute(String requestedAction, P payload, Optional<byte[]> rawBody) throws Exception {
    String actionName = normalizeAction(requestedAction);
    Action<?> configuredAction = actions.get(actionName);
    if (configuredAction == null || configuredAction.isEmpty()) {
      throw new RuntimeException(
          "No steps found for action [" + actionName + "] in [" + getName() + "] service!"
      );
    }
    BatchContext<P> context = new BatchContext<>(actionName, payload, rawBody, null);
    @SuppressWarnings("unchecked")
    Action<P> action = (Action<P>) configuredAction;
    for (BatchStep<P> step : action) {
      step.execute(context);
    }
    return true;
  }

  void onApplicationShutdown(@Observes ShutdownEvent event) {
    stop();
    receiver.close();
  }

  @Override
  public synchronized BatchStatus stop() {
    if (!consuming.get()) {
      return status();
    }

    receiver.cancel(consumerTag);
    consuming.set(false);
    LOG.infof("Stopped consuming queue %s", queueName());
    return status();
  }

  @Override
  public BatchStatus status() {
    return new BatchStatus(queueName(), consuming.get(), consumerTag);
  }
}
