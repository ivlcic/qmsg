# AGENTS.md

## Purpose

This repository contains a small Quarkus-based RabbitMQ batch framework plus two concrete batch applications:

- `batch-common`: shared framework code
- `batch-a`: sample batch app with a two-step default action
- `batch-b`: sample batch app with multiple actions and different payload types

The framework is message-driven. Each batch service:

- consumes from one RabbitMQ queue named from the service class
- can start and stop consuming through `BatchService` methods
- can expose JAX-RS status/control resources
- publishes messages to its own queue through service-level emit methods
- routes messages by action name
- deserializes a shared message envelope before step execution
- runs a configured chain of CDI-created step instances for the resolved action

## Project Layout

- [pom.xml](pom.xml): parent Maven reactor for all modules
- [batch-common](batch-common): reusable batch framework
- `com.example.batch.common.message`: message envelope and transport-neutral interfaces
- `com.example.batch.common.message.rabbitmq`: RabbitMQ implementations and header/queue helpers
- [batch-a](batch-a): first concrete batch service
- [batch-b](batch-b): second concrete batch service

## Runtime Stack

- Java `17`
- Quarkus `3.40.1.1`
- Quarkiverse RabbitMQ Client `3.4.1`
- RabbitMQ server `4.4+`, with quorum work queues for native scheduled delivery
- JAX-RS with Jackson

## Code Organization

- Use define-before-call ordering inside Java files: constructors, helper methods, nested classes, and lower-level operations should appear before methods that call them.
- For fluent or delegating APIs, place the method that does the actual work before convenience methods that delegate to it.
- Preserve this ordering when adding or refactoring framework classes.

## Core Framework

### Message Envelope

[Message.java](batch-common/src/main/java/com/example/batch/common/message/Message.java)

```json
{
  "action": "archive",
  "payload": {
    "...": "..."
  }
}
```

Behavior:

- `action` may be omitted or blank
- missing or blank action maps to the internal default key `<<default>>`
- `payload` is the typed data passed to configured steps
- `Message` also defines the functional interfaces used by the transport layer: `Reader`, `Processor`, `Writer`, `Serializer`, and `Deserializer`
- `Message.RetryMetadata` is an immutable, transport-neutral value container with validation and retry arithmetic; it is excluded from the JSON envelope with `@JsonIgnore`
- AMQP header names, parsing, and writing belong to the package-private `RabbitMQReceiver.RetryHeaders` helper, also used by `RabbitMQDeliveryApprover`

### Batch Service Contract

[BatchService.java](batch-common/src/main/java/com/example/batch/common/BatchService.java)

Public service methods:

- `getName()`
- `start()`
- `stop()`
- `status()`
- `execute(BatchContext<P> context)`

`status()` returns a `BatchStatus` with queue name, current `BatchServiceState`, and consumer tag. The service states are `initializing`, `initialized`, `stopped`, `starting`, `started`, and `stopping` in JSON.

The interface also owns the fluent action-definition API:

Use static imports for `BatchService.byDefault` and `BatchService.with`. `execute(...)` is not static; call it on the builder returned by `with(...)`.

```java
package com.example.batch.a;

import com.example.batch.a.steps.BatchACompleteStep;
import com.example.batch.a.steps.BatchAReadPayloadStep;
import com.example.batch.common.AbstractBatchService;

import static com.example.batch.common.BatchService.byDefault;
import static com.example.batch.common.BatchService.with;

public class ExampleBatchService extends AbstractBatchService {
  public ExampleBatchService() {
    super(
        byDefault(
            with(BatchAData.class).execute(
                BatchAReadPayloadStep.class,
                BatchACompleteStep.class
            )
        )
    );
  }
}
```

### Action Registry

[Actions.java](batch-common/src/main/java/com/example/batch/common/Actions.java)
[Action.java](batch-common/src/main/java/com/example/batch/common/Action.java)

`Actions` is an ordered registry from action name to `Action`.

It also holds a service-wide immutable `RetryPolicy`. The overloaded `BatchService.byDefault(ActionDefinition<P> definition, RetryPolicy retryPolicy)` registers the default action and sets its service-wide retry policy using the package-private `Actions.withRetryPolicy(...)` mutator. The existing action-definition overload uses `RetryPolicy.DEFAULT`: seven retries and delays of 5s, 15s, 45s, 120s, 300s, 900s, and 3600s.

Each `Action` contains:

- action name
- payload type
- configured step classes
- resolved CDI step instances

Step execution order is the order passed to `execute(...)` in the fluent action definition.

### Batch Service Base Class

[AbstractBatchService.java](batch-common/src/main/java/com/example/batch/common/AbstractBatchService.java)

Responsibilities:

- opens the RabbitMQ queue on startup
- starts consuming automatically on startup
- exposes `start()`, `stop()`, `status()`, `emit(...)`, and `execute(...)`
- injects all CDI `BatchStep<?>` beans
- resolves configured step classes to CDI-created instances
- normalizes blank actions to `<<default>>`
- tracks lifecycle with `BatchServiceState`
- creates the RabbitMQ receiver and emitter wiring
- executes the registered step chain for the resolved action
- acknowledges successful processing, requests redelivery only for explicit retries, and sends permanent failures to the final DLQ

Important implementation details:

- concrete services pass only an `Actions` definition to `super(...)`
- concrete service constructors no longer receive step lists
- `BatchStep` no longer owns routing metadata such as action or order
- steps should use CDI scope annotations, currently `@Dependent` in the sample apps, so each service resolves its own step instances

### Batch Step

[BatchStep.java](batch-common/src/main/java/com/example/batch/common/BatchStep.java)
[BatchResult.java](batch-common/src/main/java/com/example/batch/common/BatchResult.java)

Steps implement:

```java
package com.example.batch.a.steps;

import com.example.batch.a.BatchAData;
import com.example.batch.common.BatchContext;
import com.example.batch.common.BatchResult;
import com.example.batch.common.BatchStep;

import static com.example.batch.common.BatchStep.proceed;

public class ExampleStep implements BatchStep<BatchAData> {
  @Override
  public BatchResult execute(BatchContext<BatchAData> context) throws Exception {
    BatchAData payload = context.getPayload();
    context.put("payloadId", payload.id());
    return proceed();
  }
}
```

Routing and ordering are intentionally external to step implementations. A step only operates on its typed `BatchContext`.

Each step returns a standalone Lombok-built `BatchResult` with an explicit `BatchResult.Process`:

- `CONTINUE`: run the next step; helpers `proceed(...)` and `proceeder(...)`
- `STOP`: stop successfully and acknowledge; helpers `stop(...)` and `stopper(...)`
- `RETRY`: stop and request redelivery; helpers `retry(...)` and `retryer(...)`
- `FAIL`: stop permanently and send the message to the final DLQ; helpers `fail(...)` and `failer(...)`

The built-result shortcuts `proceed(...)`, `stop(...)`, `retry(...)`, and `fail(...)` remain on `BatchStep`. They use static imports of `BatchResult`'s builder shortcuts: `proceeder(...)`, `stopper(...)`, `retryer(...)`, and `failer(...)`. These builder helpers now belong to `BatchResult` and return `BatchResult.BatchResultBuilder` for customization before `.build()`, so they can also be used independently of steps. For example, static-import `BatchResult.retryer` and call `retryer("Try later").messageDeferMs(5000).build()`.

Logging helpers accept varargs and move a trailing `Throwable` into `cause`, removing it from `logArgs`. The cause is diagnostic only; it does not change the outcome. Failure logging defaults to `ERROR`.

Unexpected step exceptions become `fail(...)` results and follow the same logging, error-metrics, and DLQ flow as explicit failures. Null or invalid step results and unknown actions also fail permanently. Synchronous `service.execute(...)` returns `true` for successful completion or `STOP`, and `false` for `FAIL` or `RETRY`.

### Batch Context

[BatchContext.java](batch-common/src/main/java/com/example/batch/common/BatchContext.java)

Carries:

- resolved action
- typed payload
- receive timestamp
- per-message attribute map for step-to-step state sharing
- `retryCount`: number of scheduled retries preceding this delivery; initial delivery is `0`
- `totalDeferredMs`: sum of scheduled retry delays preceding this delivery
- `lastDeferredMs`: the most recent scheduled retry delay

All three retry fields default to zero for initial deliveries and synchronous calls. Their getters are `getRetryCount()`, `getTotalDeferredMs()`, and `getLastDeferredMs()`. Deferred times describe scheduled delays, excluding processing time and any additional work-queue backlog. Each step in the chain receives the same immutable retry metadata.

### Transport Interfaces

[EmitterProducer.java](batch-common/src/main/java/com/example/batch/common/message/EmitterProducer.java)
[Emitter.java](batch-common/src/main/java/com/example/batch/common/message/Emitter.java)
[Receiver.java](batch-common/src/main/java/com/example/batch/common/message/Receiver.java)

`AbstractBatchService` injects `EmitterProducer` and `Receiver` and stores an `Emitter`, without depending on RabbitMQ implementation classes. `EmitterProducer.forService(...)` creates the service-bound emitter. `Emitter` exposes the existing emit methods and close; `Receiver` exposes consumption and lifecycle operations and owns the `Controller` callback interface. None of these interfaces exposes RabbitMQ client types.

### RabbitMQ Receiver

[RabbitMQReceiver.java](batch-common/src/main/java/com/example/batch/common/message/rabbitmq/RabbitMQReceiver.java)

Implements `Receiver` as a dependent CDI bean.

Responsibilities:

- owns a separate consuming RabbitMQ `Connection` and `Channel`
- declares the work queue as durable and quorum through `RabbitMQQueues`, after checking for RabbitMQ `4.4+`
- sets `basicQos(1)`
- manages consumer registration and cancellation
- reads raw deliveries through `Message.Reader`
- processes messages through `Message.Processor`
- handles `Message.Result` dispositions `ACK`, `RETRY`, and `REJECT`
- performs `basicAck` on success
- republishes explicit retries to the same quorum queue with a future `x-opt-delivery-time` header through `RabbitMQDeliveryApprover`, then acknowledges the original only after confirmation
- forwards unreadable messages, unexpected processing exceptions, rejected results, and exhausted retries through `RabbitMQDeliveryApprover` to `queue.<Service>.dlq`
- preserves original message bytes and metadata, waits for a mandatory publish and publisher confirmation, and then acknowledges the original delivery
- leaves the original unsettled and schedules recovery if retry or DLQ forwarding fails
- validates retry metadata from AMQP headers and passes it through `Message` to `BatchContext`

Existing classic work queues must be migrated or recreated before using this version: RabbitMQ cannot change an existing queue's type through `queueDeclare`. Both receivers and emitters declare work queues with `x-queue-type=quorum`. Final DLQs are declared separately as durable queues.

### Scheduled Retries

[RabbitMQDeliveryApprover.java](batch-common/src/main/java/com/example/batch/common/message/rabbitmq/RabbitMQDeliveryApprover.java)
[RetryPolicy.java](batch-common/src/main/java/com/example/batch/common/RetryPolicy.java)
[RabbitMQQueues.java](batch-common/src/main/java/com/example/batch/common/message/rabbitmq/RabbitMQQueues.java)

No TTL queues, extra exchanges, or scheduling plugins are required. On retry, the publisher preserves the raw body and message/correlation IDs, increments the retry count, updates both deferred-time values, and sets `x-opt-delivery-time` to an absolute future Unix timestamp in milliseconds (an AMQP `long` / Java `Long`). Publishing is mandatory and persistent; the original is acknowledged only after a publisher confirmation and an unroutable-return check.

Retry headers:

- `x-batch-retry-count`
- `x-batch-total-deferred-ms`
- `x-batch-last-deferred-ms`

`RabbitMQReceiver.RetryHeaders` owns these header constants, the scheduled-delivery header `x-opt-delivery-time`, and conversion between header maps and `Message.RetryMetadata`. Header handling is kept out of the message value container. Missing metadata defaults to zero, while malformed values remain permanent failures.

The publisher removes inherited message expiration and validated publisher identity when republishing. Final-DLQ publication also removes `x-opt-delivery-time`, so rejected messages are immediately inspectable. Invalid retry headers and arithmetic overflow are permanent failures rather than retry-budget resets.

Retry defaults are defined in the service builder, as shown in both sample apps, not application configuration:

```java
import static com.example.batch.common.RetryPolicy.retryPolicy;
import static java.time.Duration.ofHours;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;

super(
    byDefault(
        with(BatchAData.class).execute(BatchAReadPayloadStep.class, BatchACompleteStep.class),
        retryPolicy()
            .maxRetries(7)
            .delays(ofSeconds(5), ofSeconds(15), ofSeconds(45),
                ofMinutes(2), ofMinutes(5), ofMinutes(15), ofHours(1))
            .build()
    )
);
```

`RetryPolicy` is a framework model in `com.example.batch.common`, not the transport package. Its Lombok builder names the retry limit and accepts human-readable `Duration` values through `delays(...)`, or a millisecond list through `delaysMs(...)`. Builder defaults match `RetryPolicy.DEFAULT`; either the limit or the delays can be overridden independently. Construction validates the policy and copies its schedule into an immutable list.

`AbstractBatchService` captures the policy in its constructor. `Receiver.Controller.retryPolicy()` exposes that same immutable object to the receiver, which passes it to the delivery approver for each retry. There is no separate transport policy or CDI policy bean. Policies can be reused across services, and subsequent builder or `Actions` changes do not alter an existing service's policy.

The limit counts retries after the initial delivery. Seven retries allow at most eight processing attempts along one retry chain. Default delays are selected by retry number; if more retries are allowed than there are delays, the final delay is reused. A positive step `messageDeferMs` overrides the default delay exactly; zero selects the service's default delay. A limit of zero sends any explicit retry directly to the final DLQ. Limits must be non-negative and the delay schedule must be non-empty and strictly positive.

### RabbitMQ Emitter

[RabbitMQEmitterProducer.java](batch-common/src/main/java/com/example/batch/common/message/rabbitmq/RabbitMQEmitterProducer.java)
[RabbitMQEmitter.java](batch-common/src/main/java/com/example/batch/common/message/rabbitmq/RabbitMQEmitter.java)

Responsibilities:

- `RabbitMQEmitterProducer` implements `EmitterProducer` as an application-scoped CDI bean and creates a service-bound `RabbitMQEmitter` via `forService(serviceName)`
- `RabbitMQEmitter` implements `Emitter` and owns one lazily opened publishing connection per service, shared by normal emits and `RabbitMQDeliveryApprover`
- uses separate channels for normal emits and retry/DLQ forwarding, with independent publishing locks and publisher confirms
- derives the destination queue from that service name
- declares the destination queue if needed
- wraps payload data into the shared `Message<P>` envelope
- publishes durable JSON messages to the target queue
- delegates serialization to the `Message.Serializer` passed to `emit(...)`

`AbstractBatchService` creates one service-bound emitter during post-construct initialization, matching the `BatchMetrics.forService(...)` style.

The receiver binds its delivery approver to that same emitter when opening. The RabbitMQ implementation requires a matching `RabbitMQEmitter`; incompatible emitter implementations are rejected before opening resources. Stopping the receiver closes only its consumer connection and the delivery approver's channel; normal emits remain available. Publishing channel failures recreate only the affected channel, while a closed publishing connection is replaced lazily. Application shutdown closes the service emitter and its shared publishing connection; a closed emitter cannot be reopened.

### Serialization

[DefaultSerializer.java](batch-common/src/main/java/com/example/batch/common/DefaultSerializer.java)
[DefaultDeserializer.java](batch-common/src/main/java/com/example/batch/common/DefaultDeserializer.java)
[DefaultMapper.java](batch-common/src/main/java/com/example/batch/common/DefaultMapper.java)

The default serializer and deserializer are Jackson-backed. The emitter no longer owns an `ObjectMapper`; callers pass the serializer used for a specific emit operation.

### Metrics

[BatchMetrics.java](batch-common/src/main/java/com/example/batch/common/BatchMetrics.java)

Quarkus Micrometer Prometheus support is enabled through `quarkus-micrometer-registry-prometheus`. Metrics are exposed by Quarkus at `/q/metrics`.

`BatchMetrics` creates a service-bound recorder for each `AbstractBatchService` instance, but metric names remain stable. Service, action, and step are labels, not dynamic metric-name segments.

Counter names:

- `batch.execution.throughput`
- `batch.execution.errors`

Both counters use the same tags:

- `service`: batch service simple class name
- `action`: resolved action name or `_all`
- `step`: configured step class simple name or `_all`

Rollups:

- overall: `action="_all"`, `step="_all"`
- overall by step: `action="_all"`, `step="<StepClass>"`
- by action over all steps: `action="<action>"`, `step="_all"`
- by action and step: `action="<action>"`, `step="<StepClass>"`

Throughput counters are incremented on successful action or step execution. Error counters are incremented when action resolution fails or a step returns `FAIL` or `RETRY`, including exceptions converted into failed results. Failed steps do not increment throughput counters.

### REST Helpers

[BatchResource.java](batch-common/src/main/java/com/example/batch/common/BatchResource.java)
[BatchStatusResource.java](batch-common/src/main/java/com/example/batch/common/BatchStatusResource.java)
[BatchControlResource.java](batch-common/src/main/java/com/example/batch/common/BatchControlResource.java)

`BatchStatusResource` adds:

- `GET /status`
- `GET /healtz`
- `GET /readyz`

`readyz` returns `200 OK` only when state is `started`; otherwise it returns `503 Service Unavailable` with the current `BatchStatus` body.

`BatchControlResource` is still available for concrete resources that want:

- `POST /start`
- `POST /stop`
- `GET /status`

## Batch A

### Module Purpose

`batch-a` is the sample service with a default action composed of two steps.

### Payload

[BatchAData.java](batch-a/src/main/java/com/example/batch/a/BatchAData.java)

```json
{
  "id": "A-1",
  "name": "example",
  "amount": 12
}
```

### Queue

- queue name: `queue.BatchAService`

### Action Mapping

[BatchAService.java](batch-a/src/main/java/com/example/batch/a/BatchAService.java)

```java
package com.example.batch.a;

import com.example.batch.a.steps.BatchACompleteStep;
import com.example.batch.a.steps.BatchAReadPayloadStep;
import com.example.batch.common.AbstractBatchService;
import jakarta.enterprise.context.ApplicationScoped;

import static com.example.batch.common.BatchService.byDefault;
import static com.example.batch.common.BatchService.with;

@ApplicationScoped
public class BatchAService extends AbstractBatchService {
  public BatchAService() {
    super(
        byDefault(
            with(BatchAData.class).execute(
                BatchAReadPayloadStep.class,
                BatchACompleteStep.class
            )
        )
    );
  }
}
```

### Steps

[BatchAReadPayloadStep.java](batch-a/src/main/java/com/example/batch/a/steps/BatchAReadPayloadStep.java)

- reads the typed payload
- logs `id`, `name`, `amount`
- stores `payloadId` into the batch context

[BatchACompleteStep.java](batch-a/src/main/java/com/example/batch/a/steps/BatchACompleteStep.java)

- reads `payloadId` from the batch context
- logs completion for that payload

### REST Resource

[BatchAResource.java](batch-a/src/main/java/com/example/batch/a/BatchAResource.java)

- `POST /batch-a/messages?action=archive-or-null`
- `GET /batch-a/messages/status`
- `GET /batch-a/messages/healtz`
- `GET /batch-a/messages/readyz`

The emit endpoint accepts the raw payload body, not the full message envelope. The service wraps it into `{ "action": ..., "payload": ... }`.

### Port And RabbitMQ Config

[application.yaml](batch-a/src/main/resources/application.yaml)

- HTTP port: `8080`
- RabbitMQ host: `localhost:5672`
- RabbitMQ user: `guest`

## Batch B

### Module Purpose

`batch-b` demonstrates multiple actions and different payload types.

### Payloads

[BatchBData1.java](batch-b/src/main/java/com/example/batch/b/BatchBData1.java)
[BatchBData2.java](batch-b/src/main/java/com/example/batch/b/BatchBData2.java)

Both current sample records use:

```json
{
  "id": "B-1",
  "category": "ops",
  "active": true
}
```

### Queue

- queue name: `queue.BatchBService`

### Action Mapping

[BatchBService.java](batch-b/src/main/java/com/example/batch/b/BatchBService.java)

- default action `<<default>>`: `BatchBDefaultStep`, `BatchBPublishStep`
- explicit action `archive`: `BatchBArchiveStep`
- explicit action `delete`: `BatchBDeleteStep`

`archive` and the default action use `BatchBData1`. `delete` uses `BatchBData2`.

### Steps

[BatchBDefaultStep.java](batch-b/src/main/java/com/example/batch/b/steps/common/BatchBDefaultStep.java)

- logs the default-action payload

[BatchBPublishStep.java](batch-b/src/main/java/com/example/batch/b/steps/publish/BatchBPublishStep.java)

- logs publish processing for the payload id

[BatchBArchiveStep.java](batch-b/src/main/java/com/example/batch/b/steps/archive/BatchBArchiveStep.java)

- logs archive processing for the payload id

[BatchBDeleteStep.java](batch-b/src/main/java/com/example/batch/b/steps/delete/BatchBDeleteStep.java)

- logs delete processing for the payload id

[BatchBPrepStep.java](batch-b/src/main/java/com/example/batch/b/steps/archive/BatchBPrepStep.java)

- exists as an archive-related step bean
- is not currently registered in `BatchBService`

### REST Resource

[BatchBResource.java](batch-b/src/main/java/com/example/batch/b/BatchBResource.java)

- async emit, default action: `POST /batch-b/exec_async` with a `BatchBData1` body
- async emit, explicit action: `POST /batch-b/exec_async/{action}` with a `BatchBData1` body
- sync execution: `POST /batch-b/exec/{action}` with a `BatchBData2` body, via `service.execute(action, requestId, payload)`
- status path: `GET /batch-b/status`
- health path: `GET /batch-b/healtz`
- readiness path: `GET /batch-b/readyz`

Async endpoints publish through `service.emit(...)` with an explicit `DefaultSerializer`. The `{action}` path parameter requires at least three word characters (`\w{3,}`).

## How To Add A New Batch Service

1. Create a new Maven module similar to `batch-a` or `batch-b`.
2. Define one or more typed payload records or classes.
3. Implement step beans as CDI beans, usually `@Dependent`, implementing `BatchStep<P>`.
4. Extend `AbstractBatchService` and pass an `Actions` registry to `super(...)`.
5. Use `byDefault(...)`, `on(...)`, `with(...)`, and `execute(...)` from `BatchService` to define actions.
6. Add a JAX-RS resource that injects the concrete service.
7. For async behavior, accept the raw payload and call `service.emit(action, payload, serializer)`.
8. For sync behavior, create or pass a `BatchContext` and call `service.execute(...)`.
9. Implement `BatchStatusResource` or `BatchControlResource` if HTTP status/start/stop endpoints are needed.
10. Add Quarkus YAML config for HTTP and RabbitMQ settings.

## Example Requests

Check Batch A status:

```bash
curl http://localhost:8080/batch-a/messages/status
```

Check Batch A health:

```bash
curl http://localhost:8080/batch-a/messages/healtz
```

Check Batch A readiness:

```bash
curl http://localhost:8080/batch-a/messages/readyz
```

Publish to Batch A default action:

```bash
curl -X POST 'http://localhost:8080/batch-a/messages' \
  -H 'Content-Type: application/json' \
  -d '{"id":"A-1","name":"example","amount":12}'
```

Check Batch B status:

```bash
curl http://localhost:8081/batch-b/status
```

Publish to Batch B archive action:

```bash
curl -X POST 'http://localhost:8081/batch-b/exec_async/archive' \
  -H 'Content-Type: application/json' \
  -d '{"id":"B-1","category":"ops","active":true}'
```

Execute Batch B delete action synchronously:

```bash
curl -X POST 'http://localhost:8081/batch-b/exec/delete' \
  -H 'Content-Type: application/json' \
  -d '{"id":"B-2","category":"ops","active":false}'
```

## Build And Run

Use a local Maven repository inside the workspace if the default home directory is not writable:

```bash
mvn -Dmaven.repo.local=.m2/repository compile
```

Run Batch A:

```bash
mvn -pl batch-a -am -Dmaven.repo.local=.m2/repository quarkus:dev
```

Run Batch B:

```bash
mvn -pl batch-b -am -Dmaven.repo.local=.m2/repository quarkus:dev
```

Prerequisites:

- RabbitMQ `4.4+` reachable at `localhost:5672`, with classic work queues migrated to quorum queues
- Maven dependency download access

## Current Caveats

- `BatchStatusResource` exposes status, healtz, and readyz only; concrete start/stop HTTP resources must extend or implement the control behavior explicitly.
- The default async reader path depends on the current `DefaultDeserializer` implementation, so typed payload deserialization should be checked carefully when adding actions with different payload types.
- Confirmed republishing followed by acknowledgement provides at-least-once delivery: a connection failure between those operations can duplicate a message. Steps with external side effects should be idempotent using the stable message/trace ID.
- Scheduled delivery uses absolute timestamps, so application and broker clocks should be synchronized. Processing can occur later than the scheduled time because of broker load or work-queue backlog.

## Verification State

Verification commands using the default local Maven cache:

```bash
mvn -q -ntp -o -DskipTests compile
mvn -q -ntp -o -Dquarkus.rabbitmqclient.devservices.enabled=false -Dmaven.test.redirectTestOutputToFile=true test
```

Framework tests run with RabbitMQ Dev Services disabled. Service tests also exercise lifecycle and publishing with interface-only fakes. `RabbitMQEmitterTest` checks shared connection ownership, channel isolation, reconnects, independent confirmation waits, and cleanup. An opt-in `RabbitMQScheduledRetryIntegrationTest` checks native delayed delivery, ready messages overtaking delayed retries, raw-payload/ID preservation, retry metadata accumulation, final-DLQ forwarding, normal emits remaining available after the delivery approver closes, and real consumption through the transport interfaces against an isolated RabbitMQ `4.4` broker.

To include that test, point these properties at a test broker:

```bash
mvn -q -ntp -Dquarkus.rabbitmqclient.devservices.enabled=false \
  -Dqmsg.rabbitmq.integration=true \
  -Dquarkus.rabbitmqclient.hostname=localhost \
  -Dquarkus.rabbitmqclient.port=5672 \
  -Dquarkus.rabbitmqclient.username=guest \
  -Dquarkus.rabbitmqclient.password=guest \
  -Dmaven.test.redirectTestOutputToFile=true clean test
```

The integration test creates and removes its own UUID-named queues. Omit `-o` when dependencies need downloading, and set `-Dmaven.repo.local=.m2/repository` when the home cache is not writable.
