package com.example.batch.common;

import com.example.batch.common.message.Emitter;
import com.example.batch.common.message.EmitterProducer;
import com.example.batch.common.message.Message;
import com.example.batch.common.message.Receiver;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.enterprise.context.control.RequestContextController;
import jakarta.enterprise.inject.Vetoed;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.example.batch.common.BatchService.*;
import static java.time.Duration.ofMillis;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Exercises the action routing and step chain execution of {@link AbstractBatchService}
 * with Quarkus-provided framework collaborators and no RabbitMQ broker.
 *
 * @author Kristijan Sečan <kristijan.secan@dropchop.com> on 16. 07. 2026.
 */
@QuarkusTest
class AbstractBatchServiceExecuteTest {

  static class TestPayload {
    final String name;

    TestPayload(String name) {
      this.name = name;
    }
  }

  /**
   * A step that records its invocations on the context and returns a configurable result.
   */
  abstract static class RecordingStep implements BatchStep<TestPayload> {
    static final String TRAIL = "trail";

    BatchResult result = BatchStep.proceed();
    RuntimeException failure;
    BatchContext<TestPayload> lastContext;

    @SuppressWarnings("unchecked")
    static List<String> trail(BatchContext<TestPayload> context) {
      List<String> trail = (List<String>) context.get(TRAIL, List.class);
      if (trail == null) {
        trail = new ArrayList<>();
        context.put(TRAIL, trail);
      }
      return trail;
    }

    @Override
    public BatchResult execute(BatchContext<TestPayload> context) {
      lastContext = context;
      trail(context).add(getClass().getSimpleName());
      if (failure != null) {
        throw failure;
      }
      return result;
    }
  }

  static class StepOne extends RecordingStep {}

  static class StepTwo extends RecordingStep {}

  static class ArchiveStep extends RecordingStep {}

  @Vetoed
  static class TestService extends AbstractBatchService {
    TestService(Actions actions) {
      super(actions);
    }
  }

  @Vetoed
  static class CapturingReceiver implements Receiver {
    Controller controller;
    boolean open;
    boolean retrierShutdown;

    @Override
    public boolean isOpen() { return open; }

    @Override
    public void scheduleStartRetry() { }

    @Override
    public void cancelStartRetry() { }

    @Override
    public long retryDelaySeconds() { return 5; }

    @Override
    public void shutdownRetrier() { retrierShutdown = true; }

    @Override
    public void close() { open = false; }

    @Override
    public void open(Controller controller, Emitter emitter) {
      this.controller = controller;
      this.open = true;
    }

    @Override
    public <M extends Message<P>, P> String consume(
        Message.Reader<M, P> reader, Message.Processor<M, P> processor) {
      return "test-consumer";
    }

    @Override
    public void cancel(String consumerTag) { }
  }

  static class RecordingEmitter implements Emitter {
    Message<?> message;
    Message.Serializer serializer;
    boolean closed;

    @Override
    public <P> void emit(Message<P> message, Message.Serializer serializer) {
      this.message = message;
      this.serializer = serializer;
    }

    @Override
    public <P> Message<P> emit(String action, P payload, Message.Serializer serializer) {
      Message<P> message = new Message<>();
      message.setAction(action);
      message.setPayload(payload);
      emit(message, serializer);
      return message;
    }

    @Override
    public void close() { closed = true; }
  }

  static class CountingRequestContextController implements RequestContextController {
    int activations;
    int deactivations;

    @Override
    public boolean activate() {
      activations++;
      return true;
    }

    @Override
    public void deactivate() {
      deactivations++;
    }
  }

  private StepOne stepOne;
  private StepTwo stepTwo;
  private ArchiveStep archiveStep;
  private CountingRequestContextController contextController;

  @Inject
  BatchMetrics batchMetrics;

  @Inject
  EmitterProducer emitterProducer;

  @Inject
  MeterRegistry registry;

  private TestService newService(Actions actions, EmitterProducer producer) {
    TestService service = new TestService(actions);
    service.batchMetrics = batchMetrics;
    service.emitterProducer = producer;
    service.requestContextController = contextController;
    service.availableSteps = List.of(stepOne, stepTwo, archiveStep);
    service.initializeSteps();
    return service;
  }

  private TestService newService(Actions actions) {
    return newService(actions, emitterProducer);
  }

  private TestService newService() {
    return newService(
        byDefault(with(TestPayload.class).execute(StepOne.class, StepTwo.class))
            .on("archive", with(TestPayload.class).execute(ArchiveStep.class))
    );
  }

  private RetryPolicy retryPolicy(TestService service) {
    CapturingReceiver receiver = new CapturingReceiver();
    service.receiver = receiver;
    service.start();
    service.getEmitter().close();
    return receiver.controller.retryPolicy();
  }

  @Test
  void lifecycleAndPublishingUseOnlyTransportInterfaces() throws Exception {
    RecordingEmitter emitter = new RecordingEmitter();
    EmitterProducer producer = serviceName -> {
      assertEquals("TestService", serviceName);
      return emitter;
    };
    TestService service = newService(byDefault(with(TestPayload.class).execute(StepOne.class)), producer);
    CapturingReceiver receiver = new CapturingReceiver();
    service.receiver = receiver;
    assertSame(emitter, service.getEmitter());

    service.start();
    assertTrue(receiver.isOpen());
    TestPayload payload = new TestPayload("example");
    Message.Serializer serializer = message -> new byte[]{1, 2, 3};
    Message<TestPayload> emitted = service.emit("archive", payload, serializer);
    assertSame(emitted, emitter.message);
    assertSame(serializer, emitter.serializer);
    assertSame(payload, emitted.getPayload());
    assertEquals("archive", emitted.getAction());

    service.stop();
    assertFalse(receiver.isOpen());
    assertFalse(emitter.closed, "stopping the receiver must leave the emitter available");
    service.emit(null, payload, serializer);
    service.onApplicationShutdown(null);
    assertTrue(receiver.retrierShutdown);
    assertTrue(emitter.closed);
  }

  @Test
  void controllerExposesThePolicySnapshotCreatedInTheServiceConstructor() {
    RetryPolicy configured = RetryPolicy.builder().maxRetries(2).delays(ofMillis(100), ofMillis(300)).build();
    Actions actions = byDefault(with(TestPayload.class).execute(StepOne.class), configured);
    TestService service = newService(actions);
    actions.withRetryPolicy(RetryPolicy.builder().maxRetries(0).build());
    RetryPolicy policy = retryPolicy(service);
    assertSame(configured, policy);
    assertEquals(2, policy.getMaxRetries());
    assertEquals(100, policy.delayMs(1, 0));
    assertEquals(300, policy.delayMs(2, 0));
    assertEquals(1234, policy.delayMs(2, 1234));
  }

  @Test
  void retryPoliciesAreServiceSpecificRatherThanApplicationScoped() {
    RetryPolicy first = retryPolicy(newService(byDefault(with(TestPayload.class).execute(StepOne.class),
        RetryPolicy.builder().maxRetries(0).delays(ofMillis(100)).build())));
    RetryPolicy second = retryPolicy(newService(byDefault(with(TestPayload.class).execute(StepOne.class),
        RetryPolicy.builder().maxRetries(4).delays(ofMillis(500)).build())));
    assertNotSame(first, second);
    assertEquals(0, first.getMaxRetries());
    assertEquals(4, second.getMaxRetries());
    assertEquals(100, first.delayMs(1, 0));
    assertEquals(500, second.delayMs(1, 0));
  }

  @Test
  void nullRetryPolicyIsRejectedAtDefinitionTime() {
    ActionDefinition<TestPayload> definition = with(TestPayload.class).execute(StepOne.class);
    assertThrows(NullPointerException.class, () -> byDefault(definition, null));
  }

  @Test
  void unchangedActionBuildersUseTheOriginalRetryDefaults() {
    RetryPolicy policy = retryPolicy(newService());
    assertSame(RetryPolicy.DEFAULT, policy);
    assertEquals(7, policy.getMaxRetries());
    assertEquals(5000, policy.delayMs(1, 0));
    assertEquals(3600000, policy.delayMs(7, 0));
  }

  private double counter(String name, String action, String step) {
    return registry.counter(
        name,
        "service", "TestService",
        "action", action,
        "step", step
    ).count();
  }

  @BeforeEach
  void setUp() {
    stepOne = new StepOne();
    stepTwo = new StepTwo();
    archiveStep = new ArchiveStep();
    registry.clear();
    contextController = new CountingRequestContextController();
  }

  @Test
  void initializationResolvesStepsAndReportsInitializedState() {
    TestService service = newService();
    assertEquals(BatchServiceState.initialized, service.status().state());
    assertEquals("TestService", service.getName());
  }

  @Test
  void initializationFailsWhenStepBeanIsMissing() {
    TestService service = new TestService(
        byDefault(with(TestPayload.class).execute(StepOne.class))
    );
    service.batchMetrics = batchMetrics;
    service.emitterProducer = emitterProducer;
    service.availableSteps = List.of(stepTwo);
    assertThrows(BatchServiceException.class, service::initializeSteps);
  }

  @Test
  void nullActionRoutesToDefaultChainInOrder() throws Exception {
    TestService service = newService();
    BatchContext<TestPayload> context = new BatchContext<>(null, "t-1", new TestPayload("p"));
    assertTrue(service.execute(context));
    assertEquals(List.of("StepOne", "StepTwo"), RecordingStep.trail(context));
  }

  @Test
  void blankActionRoutesToDefaultChain() throws Exception {
    TestService service = newService();
    BatchContext<TestPayload> context = new BatchContext<>("  ", "t-1", new TestPayload("p"));
    assertTrue(service.execute(context));
    assertEquals(List.of("StepOne", "StepTwo"), RecordingStep.trail(context));
  }

  @Test
  void namedActionRoutesToItsOwnChainOnly() throws Exception {
    TestService service = newService();
    BatchContext<TestPayload> context = new BatchContext<>("archive", "t-1", new TestPayload("p"));
    assertTrue(service.execute(context));
    assertEquals(List.of("ArchiveStep"), RecordingStep.trail(context));
  }

  @Test
  void unknownActionFailsAndRecordsExecutionError() throws Exception {
    TestService service = newService();
    BatchContext<TestPayload> context = new BatchContext<>("nope", "t-1", new TestPayload("p"));
    assertFalse(service.execute(context));
    assertEquals(1.0, counter("batch.execution.errors", "nope", "_all"));
    assertEquals(1.0, counter("batch.execution.errors", "_all", "_all"));
  }

  @Test
  void stopWithoutRetryShortCircuitsChainAndAcks() throws Exception {
    stepOne.result = BatchStep.stop("halting");
    TestService service = newService();
    BatchContext<TestPayload> context = new BatchContext<>(null, "t-1", new TestPayload("p"));
    assertTrue(service.execute(context), "stop without retry must ack (return true)");
    assertEquals(List.of("StepOne"), RecordingStep.trail(context));
  }

  @Test
  void retryStopsChainAndRequestsRedelivery() throws Exception {
    stepOne.result = BatchStep.retry("try later");
    TestService service = newService();
    BatchContext<TestPayload> context = new BatchContext<>(null, "t-1", new TestPayload("p"));
    assertFalse(service.execute(context), "retry must nack (return false)");
    assertEquals(List.of("StepOne"), RecordingStep.trail(context));
  }

  @Test
  void stopWithDiagnosticCauseRecordsSuccess() throws Exception {
    stepTwo.result = BatchStep.stop("failed [{}]", "x", new IllegalStateException("boom"));
    TestService service = newService();
    assertTrue(service.execute(new BatchContext<>(null, "t-1", new TestPayload("p"))));
    assertEquals(0.0, counter("batch.execution.errors", "<<default>>", "_all"));
    assertEquals(0.0, counter("batch.execution.errors", "<<default>>", "StepTwo"));
    assertEquals(1.0, counter("batch.execution.throughput", "<<default>>", "_all"));
  }

  @Test
  void nullStepResultFailsAndRecordsErrors() throws Exception {
    stepOne.result = null;
    TestService service = newService();
    BatchContext<TestPayload> context = new BatchContext<>(null, "t-1", new TestPayload("p"));
    assertFalse(service.execute(context));
    assertEquals(1.0, counter("batch.execution.errors", "<<default>>", "StepOne"));
  }

  @Test
  void stepExceptionBecomesFailureAndRecordsErrors() throws Exception {
    stepTwo.failure = new IllegalStateException("boom");
    TestService service = newService();
    BatchContext<TestPayload> context = new BatchContext<>(null, "t-1", new TestPayload("p"));
    assertFalse(service.execute(context));
    assertEquals(List.of("StepOne", "StepTwo"), RecordingStep.trail(context));
    assertEquals(1.0, counter("batch.execution.errors", "<<default>>", "_all"));
    assertEquals(1.0, counter("batch.execution.errors", "<<default>>", "StepTwo"));
    // StepOne completed before the failure, so its throughput was still recorded.
    assertEquals(1.0, counter("batch.execution.throughput", "<<default>>", "StepOne"));
    assertEquals(0.0, counter("batch.execution.throughput", "<<default>>", "_all"));
  }

  @Test
  void successfulExecutionRecordsThroughputPerStepAndRollup() throws Exception {
    TestService service = newService();
    assertTrue(service.execute(new BatchContext<>(null, "t-1", new TestPayload("p"))));
    assertEquals(1.0, counter("batch.execution.throughput", "<<default>>", "_all"));
    assertEquals(1.0, counter("batch.execution.throughput", "<<default>>", "StepOne"));
    assertEquals(1.0, counter("batch.execution.throughput", "<<default>>", "StepTwo"));
    assertEquals(1.0, counter("batch.execution.throughput", "_all", "_all"));
    assertEquals(1.0, counter("batch.execution.throughput", "_all", "StepOne"));
    assertEquals(0.0, counter("batch.execution.errors", "_all", "_all"));
  }

  @Test
  void executionActivatesAndDeactivatesRequestContext() throws Exception {
    TestService service = newService();
    service.execute(new BatchContext<>(null, "t-1", new TestPayload("p")));
    assertEquals(1, contextController.activations);
    assertEquals(1, contextController.deactivations);
  }

  @Test
  void requestContextIsDeactivatedEvenWhenStepThrows() throws Exception {
    stepOne.failure = new IllegalStateException("boom");
    TestService service = newService();
    assertFalse(service.execute(new BatchContext<>(null, "t-1", new TestPayload("p"))));
    assertEquals(1, contextController.activations);
    assertEquals(1, contextController.deactivations);
  }

  @Test
  void convenienceOverloadBuildsSynchronousContext() throws Exception {
    TestService service = newService();
    assertTrue(service.execute("archive", "req-1", new TestPayload("p")));
    assertEquals(1.0, counter("batch.execution.throughput", "archive", "_all"));
  }

  private Message.Result process(TestService service) throws Exception {
    Message<TestPayload> message = new Message<>();
    message.setId("t-1");
    message.setPayload(new TestPayload("p"));
    Message.Processor<Message<TestPayload>, TestPayload> processor = service.getProcessor();
    return processor.process(message);
  }

  @Test
  void asyncSuccessfulStopAcknowledges() throws Exception {
    stepOne.result = BatchStep.stop("nothing to do");
    assertEquals(Message.Disposition.ACK, process(newService()).disposition());
  }

  @Test
  void asyncFailRejectsPermanently() throws Exception {
    RuntimeException failure = new IllegalArgumentException("invalid item");
    stepOne.result = BatchStep.fail("invalid", failure);
    Message.Result result = process(newService());
    assertEquals(Message.Disposition.REJECT, result.disposition());
    assertSame(failure, result.cause());
  }

  @Test
  void explicitFailureWithoutCauseStopsChainAndRecordsErrors() throws Exception {
    stepOne.result = BatchStep.fail("invalid item");
    TestService service = newService();
    BatchContext<TestPayload> context = new BatchContext<>(null, "t-1", new TestPayload("p"));
    assertFalse(service.execute(context));
    assertEquals(List.of("StepOne"), RecordingStep.trail(context));
    assertEquals(1.0, counter("batch.execution.errors", "<<default>>", "_all"));
    assertEquals(1.0, counter("batch.execution.errors", "<<default>>", "StepOne"));
    assertEquals(0.0, counter("batch.execution.throughput", "<<default>>", "StepOne"));
    assertEquals(0.0, counter("batch.execution.throughput", "<<default>>", "_all"));
  }

  @Test
  void asyncFailWithoutCauseRejectsPermanently() throws Exception {
    stepOne.result = BatchStep.fail("invalid item");
    Message.Result result = process(newService());
    assertEquals(Message.Disposition.REJECT, result.disposition());
    assertNull(result.cause());
  }

  @Test
  void asyncStopWithDiagnosticCauseAcknowledges() throws Exception {
    stepOne.result = BatchStep.stop("nothing to do", new IllegalStateException("diagnostic"));
    assertEquals(Message.Disposition.ACK, process(newService()).disposition());
  }

  @Test
  void asyncUnexpectedExceptionUsesFailureOutcomeAndPreservesCause() throws Exception {
    RuntimeException failure = new IllegalStateException("unexpected");
    stepOne.failure = failure;
    Message.Result result = process(newService());
    assertEquals(Message.Disposition.REJECT, result.disposition());
    assertSame(failure, result.cause());
    assertEquals(1.0, counter("batch.execution.errors", "<<default>>", "_all"));
    assertEquals(1.0, counter("batch.execution.errors", "<<default>>", "StepOne"));
    assertEquals(0.0, counter("batch.execution.throughput", "<<default>>", "StepOne"));
    assertEquals(0.0, counter("batch.execution.throughput", "<<default>>", "StepTwo"));
    assertEquals(1, contextController.deactivations);
  }

  @Test
  void asyncUnknownActionIsRejected() throws Exception {
    Message<TestPayload> message = new Message<>();
    message.setId("t-1");
    message.setAction("nope");
    message.setPayload(new TestPayload("p"));
    Message.Processor<Message<TestPayload>, TestPayload> processor = newService().getProcessor();
    assertEquals(Message.Disposition.REJECT, processor.process(message).disposition());
    assertEquals(1.0, counter("batch.execution.errors", "nope", "_all"));
  }

  @Test
  void invalidStepOutcomeFailsAndStopsChain() throws Exception {
    stepOne.result = BatchResult.builder().process(null).build();
    TestService service = newService();
    BatchContext<TestPayload> context = new BatchContext<>(null, "t-1", new TestPayload("p"));
    assertFalse(service.execute(context));
    assertEquals(List.of("StepOne"), RecordingStep.trail(context));
    assertEquals(1.0, counter("batch.execution.errors", "<<default>>", "StepOne"));
  }

  @Test
  void nullContextReturnsFailure() throws Exception {
    assertFalse(newService().execute((BatchContext<TestPayload>) null));
    assertEquals(1.0, counter("batch.execution.errors", "<<default>>", "_all"));
  }

  @Test
  void requestContextActivationExceptionBecomesFailure() throws Exception {
    RuntimeException failure = new IllegalStateException("context unavailable");
    TestService service = newService();
    service.requestContextController = new RequestContextController() {
      public boolean activate() { throw failure; }
      public void deactivate() { throw new AssertionError("context was not activated"); }
    };
    Message.Result result = process(service);
    assertEquals(Message.Disposition.REJECT, result.disposition());
    assertSame(failure, result.cause());
    assertEquals(1.0, counter("batch.execution.errors", "<<default>>", "_all"));
  }

  @Test
  void asyncRetryPreservesRequestedDelayAndCause() throws Exception {
    RuntimeException failure = new IllegalStateException("temporarily unavailable");
    stepOne.result = BatchResult.retryer("try later", failure).messageDeferMs(7000).build();
    Message.Result result = process(newService());
    assertEquals(Message.Disposition.RETRY, result.disposition());
    assertEquals(7000, result.deferMs());
    assertSame(failure, result.cause());
  }

  @Test
  void asyncExecutionPassesRetryAndDeferredMetadataToEveryStep() throws Exception {
    Message<TestPayload> message = new Message<>();
    message.setId("trace-1");
    message.setPayload(new TestPayload("p"));
    message.setRetryMetadata(new Message.RetryMetadata(2, 20000, 15000));
    Message.Processor<Message<TestPayload>, TestPayload> processor = newService().getProcessor();
    assertEquals(Message.Disposition.ACK, processor.process(message).disposition());
    assertEquals(2, stepOne.lastContext.getRetryCount());
    assertEquals(20000, stepOne.lastContext.getTotalDeferredMs());
    assertEquals(15000, stepOne.lastContext.getLastDeferredMs());
    assertSame(stepOne.lastContext, stepTwo.lastContext);
    assertTrue(stepOne.lastContext.isAsync());
  }
}
