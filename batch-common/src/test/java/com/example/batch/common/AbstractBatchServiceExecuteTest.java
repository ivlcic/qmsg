package com.example.batch.common;

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

    BatchStep.Result result = BatchStep.proceed();
    RuntimeException failure;

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
    public Result execute(BatchContext<TestPayload> context) {
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
  BatchEmitter batchEmitter;

  @Inject
  MeterRegistry registry;

  private TestService newService(Actions actions) {
    TestService service = new TestService(actions);
    service.batchMetrics = batchMetrics;
    service.batchEmitter = batchEmitter;
    service.requestContextController = contextController;
    service.availableSteps = List.of(stepOne, stepTwo, archiveStep);
    service.initializeSteps();
    return service;
  }

  private TestService newService() {
    return newService(
        byDefault(with(TestPayload.class).execute(StepOne.class, StepTwo.class))
            .on("archive", with(TestPayload.class).execute(ArchiveStep.class))
    );
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
    service.batchEmitter = batchEmitter;
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
    stepOne.result = BatchStep.Result.builder().process(null).build();
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
    stepOne.result = BatchStep.retryer("try later", failure).messageDeferMs(7000).build();
    Message.Result result = process(newService());
    assertEquals(Message.Disposition.RETRY, result.disposition());
    assertEquals(7000, result.deferMs());
    assertSame(failure, result.cause());
  }
}
