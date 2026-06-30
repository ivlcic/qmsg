package com.example.batch.common;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.arc.All;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractBatchService {
  private static final Logger LOG = Logger.getLogger(AbstractBatchService.class);
  static final String DEFAULT_ACTION = "<<default>>";

  @Inject
  MessageClientReceiver receiver;

  @Inject
  ObjectMapper objectMapper;


  @Inject @All
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
        return (S)step;
      }
    }
    throw new IllegalStateException("No batch step bean found for " + type.getName());
  }

  @PostConstruct
  void initializeSteps() {
    for (Action<?> action : actions.values()) {
      initializeAction(action);
    }
  }

  private <P> void initializeAction(Action<P> action) {
    for (Class<? extends BatchStep<P>> stepType : action.stepTypes()) {
      action.addStep(resolveStep(stepType));
    }
  }

  public String getName() {
    return getClass().getSimpleName();
  }

  protected String queueName() {
    return "queue." + getName();
  }

  public void onApplicationStart(@Observes StartupEvent event) {
    receiver.open(queueName());
    start();
  }

  public synchronized BatchStatus start() {
    if (consuming.get()) {
      return status();
    }

    receiver.open(queueName());
    consumerTag = receiver.consume(this::handleMessage, tag -> {
      consuming.set(false);
      LOG.infof("Consumer for queue %s was cancelled by the broker", queueName());
    });
    consuming.set(true);
    LOG.infof("Started consuming queue %s with consumer tag %s", queueName(), consumerTag);
    return status();
  }

  private boolean handleMessage(byte[] body) {
    try {
      Message<JsonNode> message = objectMapper.readValue(
          body, new TypeReference<Message<JsonNode>>() {}
      );

      String action = normalizeAction(message.getAction());
      Action<?> configuredAction = actions.get(action);
      if (configuredAction == null || configuredAction.isEmpty()) {
        throw new RuntimeException(
            "No steps found for action [" + action + "] in [" + getName() + "] service!"
        );
      }
      executeAction(configuredAction, message.getPayload(), body);
      return true;
    } catch (Exception e) {
      LOG.errorf(e, "Failed to process message from queue %s", queueName());
      return false;
    }
  }

  private <P> void executeAction(
      Action<P> action,
      JsonNode payload,
      byte[] body) throws Exception {
    P typedPayload = payload == null || payload.isNull()
        ? null
        : objectMapper.treeToValue(payload, action.payloadType());
    BatchContext<P> context = new BatchContext<>(action.name(), typedPayload, body, null);
    for (BatchStep<P> step : action) {
      step.execute(context);
    }
  }

  public void onApplicationShutdown(@Observes ShutdownEvent event) {
    stop();
    receiver.close();
  }

  public synchronized BatchStatus stop() {
    if (!consuming.get()) {
      return status();
    }

    receiver.cancel(consumerTag);
    consuming.set(false);
    LOG.infof("Stopped consuming queue %s", queueName());
    return status();
  }

  public BatchStatus status() {
    return new BatchStatus(queueName(), consuming.get(), consumerTag);
  }

  private static String normalizeAction(String action) {
    if (action == null || action.isBlank()) {
      return DEFAULT_ACTION;
    }
    return action;
  }
}
