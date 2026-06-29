package com.example.batch.common;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public abstract class AbstractBatchService<P> {
    private static final Logger LOG = Logger.getLogger(AbstractBatchService.class);
    static final String DEFAULT_ACTION = "<<default>>";

    @Inject
    MessageClientReceiver receiver;

    @Inject
    ObjectMapper objectMapper;

    private final AtomicBoolean consuming = new AtomicBoolean(false);
    private final ActionSteps<P> steps;
    private String consumerTag;


    private BatchStep<P> resolveStep(Class<? extends BatchStep<P>> type, List<BatchStep<P>> availableSteps) {
        for (BatchStep<P> step : availableSteps) {
            if (type.isInstance(step)) {
                return step;
            }
        }
        throw new IllegalStateException("No batch step bean found for " + type.getName());
    }

    public AbstractBatchService(List<BatchStep<P>> availableSteps,
                                ActionStepTypes<P> actionMap) {
        this.steps = new ActionSteps<>();
        for (Map.Entry<String, StepTypes<P>> entry : actionMap.entrySet()) {
            this.steps.put(
                entry.getKey(),
                new Steps<>(entry.getValue().stream()
                    .map(type -> resolveStep(type, availableSteps))
                    .collect(Collectors.toList()))
                );
        }
    }

    public String getName() {
        return getClass().getSimpleName();
    }

    protected String queueName() {
        return "queue." + getName();
    }

    protected ActionSteps<P> steps() {
        return this.steps;
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
            Message<P> message = objectMapper.readValue(new String(body, StandardCharsets.UTF_8), new TypeReference<>(){});

            String action = message.getAction();
            if (action == null || action.isBlank()) {
                action = DEFAULT_ACTION;
            }
            Steps<P> actionSteps = steps.get(action);
            if (actionSteps == null || actionSteps.isEmpty()) {
                throw new RuntimeException(
                    "No steps found for action [" + action + "] in [" + getName() + "] service!"
                );
            }
            BatchContext<P> context = new BatchContext<>(action, message.getPayload(), body, null);
            for (BatchStep<P> step : actionSteps) {
                step.execute(context);
            }
            return true;
        } catch (Exception e) {
            LOG.errorf(e, "Failed to process message from queue %s", queueName());
            return false;
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
}
