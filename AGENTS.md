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
- [batch-a](batch-a): first concrete batch service
- [batch-b](batch-b): second concrete batch service

## Runtime Stack

- Java `17`
- Quarkus `3.35.4`
- Quarkiverse RabbitMQ Client `3.3.0`
- JAX-RS with Jackson

## Code Organization

- Use define-before-call ordering inside Java files: constructors, helper methods, nested classes, and lower-level operations should appear before methods that call them.
- For fluent or delegating APIs, place the method that does the actual work before convenience methods that delegate to it.
- Preserve this ordering when adding or refactoring framework classes.

## Core Framework

### Message Envelope

[Message.java](batch-common/src/main/java/com/example/batch/common/Message.java)

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
- returns `ack` on successful processing and `nack` with requeue on failure

Important implementation details:

- concrete services pass only an `Actions` definition to `super(...)`
- concrete service constructors no longer receive step lists
- `BatchStep` no longer owns routing metadata such as action or order
- steps should use CDI scope annotations, currently `@Dependent` in the sample apps, so each service resolves its own step instances

### Batch Step

[BatchStep.java](batch-common/src/main/java/com/example/batch/common/BatchStep.java)

Steps implement:

```java
package com.example.batch.a.steps;

import com.example.batch.a.BatchAData;
import com.example.batch.common.BatchContext;
import com.example.batch.common.BatchStep;

public class ExampleStep implements BatchStep<BatchAData> {
  @Override
  public void execute(BatchContext<BatchAData> context) throws Exception {
    BatchAData payload = context.payload();
    context.put("payloadId", payload.id());
  }
}
```

Routing and ordering are intentionally external to step implementations. A step only operates on its typed `BatchContext`.

### Batch Context

[BatchContext.java](batch-common/src/main/java/com/example/batch/common/BatchContext.java)

Carries:

- resolved action
- typed payload
- receive timestamp
- per-message attribute map for step-to-step state sharing

### RabbitMQ Receiver

[BatchReceiver.java](batch-common/src/main/java/com/example/batch/common/BatchReceiver.java)

Responsibilities:

- owns RabbitMQ `Connection` and `Channel`
- declares the queue
- sets `basicQos(1)`
- manages consumer registration and cancellation
- reads raw deliveries through `Message.Reader`
- processes messages through `Message.Processor`
- performs `basicAck` or `basicNack(..., requeue=true)` based on processor result

### RabbitMQ Emitter

[BatchEmitter.java](batch-common/src/main/java/com/example/batch/common/BatchEmitter.java)

Responsibilities:

- creates a service-bound `ServiceEmitter` via `forService(serviceName)`
- derives the destination queue from that service name
- declares the destination queue if needed
- wraps payload data into the shared `Message<P>` envelope
- publishes durable JSON messages to the target queue
- delegates serialization to the `Message.Serializer` passed to `emit(...)`

`AbstractBatchService` creates one service-bound emitter during post-construct initialization, matching the `BatchMetrics.forService(...)` style.

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

Throughput counters are incremented on successful action or step execution. Error counters are incremented when action resolution fails or a step throws.

### REST Helpers

[BatchResource.java](batch-common/src/main/java/com/example/batch/common/BatchResource.java)
[BatchStatusResource.java](batch-common/src/main/java/com/example/batch/common/BatchStatusResource.java)
[AbstractBatchControlResource.java](batch-common/src/main/java/com/example/batch/common/AbstractBatchControlResource.java)

`BatchStatusResource` adds:

- `GET /status`
- `GET /healtz`
- `GET /readyz`

`readyz` returns `200 OK` only when state is `started`; otherwise it returns `503 Service Unavailable` with the current `BatchStatus` body.

`AbstractBatchControlResource` is still available for concrete resources that want:

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

[BatchAEmitResource.java](batch-a/src/main/java/com/example/batch/a/BatchAEmitResource.java)

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

- base path: `POST /batch-b`
- status path: `GET /batch-b/status`
- health path: `GET /batch-b/healtz`
- readiness path: `GET /batch-b/readyz`
- exposes async emit methods through `service.emit(...)`
- exposes sync execution methods through `service.execute(...)`

## How To Add A New Batch Service

1. Create a new Maven module similar to `batch-a` or `batch-b`.
2. Define one or more typed payload records or classes.
3. Implement step beans as CDI beans, usually `@Dependent`, implementing `BatchStep<P>`.
4. Extend `AbstractBatchService` and pass an `Actions` registry to `super(...)`.
5. Use `byDefault(...)`, `on(...)`, `with(...)`, and `execute(...)` from `BatchService` to define actions.
6. Add a JAX-RS resource that injects the concrete service.
7. For async behavior, accept the raw payload and call `service.emit(action, payload, serializer)`.
8. For sync behavior, create or pass a `BatchContext` and call `service.execute(...)`.
9. Implement `BatchStatusResource` or extend `AbstractBatchControlResource` if HTTP status/start/stop endpoints are needed.
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
curl -X POST 'http://localhost:8081/batch-b?action=archive' \
  -H 'Content-Type: application/json' \
  -d '{"id":"B-1","category":"ops","active":true}'
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

- RabbitMQ reachable at `localhost:5672`
- Maven dependency download access

## Current Caveats

- `BatchStatusResource` exposes status, healtz, and readyz only; concrete start/stop HTTP resources must extend or implement the control behavior explicitly.
- The default async reader path depends on the current `DefaultDeserializer` implementation, so typed payload deserialization should be checked carefully when adding actions with different payload types.
- `BatchBResource` currently overloads JAX-RS `POST` methods by Java payload type. That may need explicit subpaths or media-type separation if runtime endpoint selection becomes ambiguous.
- On processing failure the message is requeued, so poison messages can loop indefinitely without a dead-letter strategy.

## Verification State

This file reflects the current source layout and API shape after the framework refactor. The repository was compiled successfully with:

```bash
mvn -q -Dmaven.repo.local=.m2/repository -DskipTests compile
```

Tests were not run.
