# AGENTS.md

## Purpose

This repository contains a small Quarkus-based RabbitMQ batch framework plus two concrete batch applications:

- `batch-common`: shared framework code
- `batch-a`: sample batch app with one default action made of two steps
- `batch-b`: sample batch app with one default action and one explicit `archive` action

The framework is message-driven. Each batch app:

- consumes from one RabbitMQ queue
- can start and stop consuming through JAX-RS control endpoints
- publishes messages to its own queue through a JAX-RS emit endpoint
- routes messages by action name
- deserializes each action payload into that action's configured payload type
- executes an ordered list of CDI step instances for the resolved action

## Project Layout

- [pom.xml](pom.xml): parent Maven reactor
- [batch-common](batch-common): reusable framework module
- [batch-a](batch-a): sample Batch A application
- [batch-b](batch-b): sample Batch B application

## Runtime Stack

- Java `17`
- Quarkus `3.35.4`
- Quarkiverse RabbitMQ Client `3.3.0`
- JAX-RS with Jackson

## Core Framework

### Message Envelope

[Message.java](batch-common/src/main/java/com/example/batch/common/Message.java)

Messages use a shared envelope:

```json
{
  "action": "archive",
  "payload": {
    "...": "..."
  }
}
```

Behavior:

- `action` may be omitted or blank.
- Missing or blank `action` maps to `BatchService.DEFAULT_ACTION`, currently `<<default>>`.
- The envelope is first read as `Message<JsonNode>`.
- After action resolution, `payload` is deserialized to the payload type declared for that action.
- `Message.Receiver` is the functional callback used by `BatchClientReceiver` for raw body handling.
- `Message.Emitter` is the functional callback used by `BatchClientEmitter` for raw body emission.
- `Message.Serializer` converts a message envelope to bytes.
- `Message.Deserializer<P>` converts bytes to a typed message envelope.

### Batch Service Contract

[BatchService.java](batch-common/src/main/java/com/example/batch/common/BatchService.java)

`BatchService` is the public service contract. It exposes:

- `getName()`
- `start()`
- `stop()`
- `status()`

It also owns the action DSL static entry points:

```java
public class BatchBService extends AbstractBatchService {

  public BatchBService() {
    super(
        byDefault(
            with(BatchBData.class)
                .execute(BatchBDefaultStep.class)
        ).on(
            "archive",
            with(BatchBData.class)
                .execute(BatchBArchiveStep.class)
        )
    );
  }
}
```

DSL structure:

- `with(PayloadType.class)` creates a typed action builder.
- `.execute(StepOne.class, StepTwo.class)` creates an unnamed action definition.
- `byDefault(...)` creates an `Actions` registry with the default action.
- `.on("name", ...)` adds another named action to the registry.

`ActionBuilder` and `ActionDefinition` are nested classes inside `BatchService`, not standalone files.

### Actions And Action

[Actions.java](batch-common/src/main/java/com/example/batch/common/Actions.java)
[Action.java](batch-common/src/main/java/com/example/batch/common/Action.java)

`Actions` is the action registry. It is an instance-only fluent API:

- `byDefault(ActionDefinition<P>)`
- `on(String, ActionDefinition<P>)`

`Action<P>` is the action container. It owns:

- action name
- payload type
- configured step classes
- resolved CDI step instances

Step order is the order declared in `.execute(...)`. There is no `BatchStep.action()` and no `BatchStep.order()`.

### Batch Service Base Class

[AbstractBatchService.java](batch-common/src/main/java/com/example/batch/common/AbstractBatchService.java)

Responsibilities:

- stores the configured `Actions`
- injects all available CDI `BatchStep<?>` beans using `@All`
- resolves configured step classes to CDI step instances at `@PostConstruct`
- opens the queue and starts consuming on Quarkus startup
- exposes `start()`, `stop()`, and `status()` through the `BatchService` contract
- normalizes missing or blank actions to `<<default>>`
- deserializes payload per resolved action
- creates `BatchContext<P>` and executes the action steps
- returns `ack` on success
- returns `nack` with requeue on failure

Important current behavior:

- `AbstractBatchService` is intentionally not generic because one service may support actions with different payload types.
- Payload typing belongs to each configured action, not to the whole service.
- Step instances are resolved from CDI, not manually constructed.

### Batch Steps

[BatchStep.java](batch-common/src/main/java/com/example/batch/common/BatchStep.java)

Steps implement:

```java
public interface BatchStep<P> {
  void execute(BatchContext<P> context) throws Exception;
}
```

Conventions:

- Concrete step beans should be `@Dependent`.
- `@Dependent` keeps step instances scoped to the owning injection context instead of sharing a single application-wide step instance.
- A single action chain must use one payload type across all of its steps.
- Different actions in the same service may use different payload types.

### Batch Client Receiver

[BatchClientReceiver.java](batch-common/src/main/java/com/example/batch/common/BatchClientReceiver.java)

Responsibilities:

- owns RabbitMQ `Connection` and `Channel`
- declares the queue
- sets `basicQos(1)`
- manages consumer registration and cancellation
- performs `basicAck` or `basicNack(..., requeue=true)` based on handler result

### Batch Context

[BatchContext.java](batch-common/src/main/java/com/example/batch/common/BatchContext.java)

Carries per-message execution state:

- resolved action name
- typed payload
- raw message body
- RabbitMQ message properties field
- receive timestamp
- per-message attributes map for sharing state between steps

Current note: `AbstractBatchService` currently passes `null` for RabbitMQ properties because the reader handler contract only passes message body bytes.

### Shared REST Control Resource

[AbstractBatchControlResource.java](batch-common/src/main/java/com/example/batch/common/AbstractBatchControlResource.java)

Exposes:

- `POST /.../start`
- `POST /.../stop`
- `GET /.../status`

The resource depends on the `BatchService` interface, not directly on `AbstractBatchService`.

### Batch Client Emitter

[BatchClientEmitter.java](batch-common/src/main/java/com/example/batch/common/BatchClientEmitter.java)

Responsibilities:

- creates the destination queue if needed
- emits serialized message bytes to the target queue
- derives the queue name from the service name constructor parameter as `queue.<serviceName>`

Current behavior:

- opens a RabbitMQ connection and channel per publish call
- sets `contentType` to `application/json`
- sets `deliveryMode(2)` for persistent messages
- does not own Jackson serialization; callers pass `Message.Serializer` to `publish(...)`

Emitter injection is generic. Use `@ForBatchService(ServiceClass.class)` at the injection point:

```java
@Inject
@ForBatchService(BatchAService.class)
BatchClientEmitter emitter;
```

[BatchClientEmitterProducer.java](batch-common/src/main/java/com/example/batch/common/BatchClientEmitterProducer.java) reads the qualifier from the injection point and creates an emitter for that service name.

## Batch A

### Purpose

`batch-a` demonstrates a default action with two ordered steps.

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
- emitters targeting this queue inject `@ForBatchService(BatchAService.class) BatchClientEmitter`

### Action Mapping

[BatchAService.java](batch-a/src/main/java/com/example/batch/a/BatchAService.java)

```java
public class BatchAService extends AbstractBatchService {

  public BatchAService() {
    super(
        byDefault(
            with(BatchAData.class)
                .execute(
                    BatchAReadPayloadStep.class,
                    BatchACompleteStep.class
                )
        )
    );
  }
}
```

Default action steps:

- `BatchAReadPayloadStep`
- `BatchACompleteStep`

### Steps

[BatchAReadPayloadStep.java](batch-a/src/main/java/com/example/batch/a/steps/BatchAReadPayloadStep.java)

- `@Dependent`
- reads the typed `BatchAData` payload
- logs `id`, `name`, and `amount`
- stores `payloadId` in the context

[BatchACompleteStep.java](batch-a/src/main/java/com/example/batch/a/steps/BatchACompleteStep.java)

- `@Dependent`
- reads `payloadId` from the context
- logs completion

### REST Endpoints

[BatchAControlResource.java](batch-a/src/main/java/com/example/batch/a/BatchAControlResource.java)
[BatchAEmitResource.java](batch-a/src/main/java/com/example/batch/a/BatchAEmitResource.java)

- `POST /batch-a/control/start`
- `POST /batch-a/control/stop`
- `GET /batch-a/control/status`
- `POST /batch-a/messages?action=action-or-null`

Publishing body for `POST /batch-a/messages`:

```json
{
  "id": "A-1",
  "name": "example",
  "amount": 12
}
```

The emit resource accepts the raw payload body, wraps it into a `Message<BatchAData>`, and passes a `Message.Serializer` to the generic emitter.

### Config

[application.yaml](batch-a/src/main/resources/application.yaml)

- HTTP port: `8080`
- RabbitMQ host: `localhost:5672`
- RabbitMQ user: `guest`

## Batch B

### Purpose

`batch-b` demonstrates one default action and one explicit action.

### Payload

[BatchBData.java](batch-b/src/main/java/com/example/batch/b/BatchBData.java)

```json
{
  "id": "B-1",
  "category": "ops",
  "active": true
}
```

### Queue

- queue name: `queue.BatchBService`
- emitters targeting this queue inject `@ForBatchService(BatchBService.class) BatchClientEmitter`

### Action Mapping

[BatchBService.java](batch-b/src/main/java/com/example/batch/b/BatchBService.java)

```java
public class BatchBService extends AbstractBatchService {

  public BatchBService() {
    super(
        byDefault(
            with(BatchBData.class)
                .execute(BatchBDefaultStep.class)
        ).on(
            "archive",
            with(BatchBData.class)
                .execute(BatchBArchiveStep.class)
        )
    );
  }
}
```

Actions:

- default action `<<default>>`: `BatchBDefaultStep`
- explicit action `archive`: `BatchBArchiveStep`

### Steps

[BatchBDefaultStep.java](batch-b/src/main/java/com/example/batch/b/steps/BatchBDefaultStep.java)

- `@Dependent`
- logs typed `BatchBData` fields

[BatchBArchiveStep.java](batch-b/src/main/java/com/example/batch/b/steps/BatchBArchiveStep.java)

- `@Dependent`
- logs archive processing for the payload id

### REST Endpoints

[BatchBControlResource.java](batch-b/src/main/java/com/example/batch/b/BatchBControlResource.java)
[BatchBEmitResource.java](batch-b/src/main/java/com/example/batch/b/BatchBEmitResource.java)

- `POST /batch-b/control/start`
- `POST /batch-b/control/stop`
- `GET /batch-b/control/status`
- `POST /batch-b/messages?action=archive-or-null`

Publishing body for `POST /batch-b/messages`:

```json
{
  "id": "B-1",
  "category": "ops",
  "active": true
}
```

### Config

[application.yaml](batch-b/src/main/resources/application.yaml)

- HTTP port: `8081`
- RabbitMQ host: `localhost:5672`
- RabbitMQ user: `guest`

## How To Add A New Batch Service

1. Create a Maven module similar to `batch-a` or `batch-b`.
2. Define one or more payload records/classes, one per action shape if needed.
3. Implement step beans as `@Dependent` `BatchStep<P>`.
4. Extend `AbstractBatchService`.
5. Pass an `Actions` registry to `super(...)` using `BatchService.byDefault`, `BatchService.with`, and chained `Actions.on`.
6. Inject `@ForBatchService(MyBatchService.class) BatchClientEmitter` where messages need to be emitted.
7. Add a control resource by extending `AbstractBatchControlResource` and returning the service as `BatchService`.
8. Add an emit resource that accepts a raw payload, creates a `Message<P>`, and calls `emitter.publish(message, serializer)`.
9. Add Quarkus YAML config for HTTP and RabbitMQ settings.

Example:

```java
public class MyBatchService extends AbstractBatchService {
  public MyBatchService() {
    super(
        byDefault(
            with(DefaultPayload.class)
                .execute(DefaultStepOne.class, DefaultStepTwo.class)
        ).on(
            "archive",
            with(ArchivePayload.class)
                .execute(ArchiveStep.class)
        )
    );
  }
}
```

## Example Requests

Start Batch A consumer:

```bash
curl -X POST http://localhost:8080/batch-a/control/start
```

Stop Batch A consumer:

```bash
curl -X POST http://localhost:8080/batch-a/control/stop
```

Check Batch A status:

```bash
curl http://localhost:8080/batch-a/control/status
```

Publish to Batch A default action:

```bash
curl -X POST 'http://localhost:8080/batch-a/messages' \
  -H 'Content-Type: application/json' \
  -d '{"id":"A-1","name":"example","amount":12}'
```

Publish to Batch B archive action:

```bash
curl -X POST 'http://localhost:8081/batch-b/messages?action=archive' \
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

- On processing failure the message is requeued, so poison messages can loop indefinitely without a dead-letter strategy.
- `BatchContext.properties()` currently exists, but RabbitMQ delivery properties are not passed from `BatchClientReceiver` into `AbstractBatchService`.
- Emit resources are currently typed to a single payload class per app. If one service exposes multiple actions with different payload types through one endpoint, the emit layer will need to accept raw JSON or expose action-specific endpoints.
- `BatchClientEmitter` opens a connection and channel per publish call.

## Verification State

The repository was compiled successfully with:

```bash
mvn -q -DskipTests compile
```

Tests were not run.
