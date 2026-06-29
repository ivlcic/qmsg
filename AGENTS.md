# AGENTS.md

## Purpose

This repository contains a small Quarkus-based RabbitMQ batch framework plus two concrete batch applications:

- `batch-common`: shared framework code
- `batch-a`: sample batch app with a two-step default action
- `batch-b`: sample batch app with a default action and an explicit `archive` action

The framework is message-driven. Each batch app:

- consumes from one RabbitMQ queue
- can start and stop consuming through JAX-RS endpoints
- publishes messages to its own queue through a JAX-RS endpoint
- deserializes JSON into a typed payload before step execution
- runs a configured chain of injectable steps based on message action

## Project Layout

- [pom.xml](/home/nikola/tmp/qmsg/pom.xml): parent Maven reactor for all modules
- [batch-common](/home/nikola/tmp/qmsg/batch-common): reusable batch framework
- [batch-a](/home/nikola/tmp/qmsg/batch-a): first concrete batch service
- [batch-b](/home/nikola/tmp/qmsg/batch-b): second concrete batch service

## Runtime Stack

- Java `17`
- Quarkus `3.35.4`
- Quarkiverse RabbitMQ Client `3.3.0`
- JAX-RS with Jackson

## Core Framework

### Shared message envelope

[Message.java](/home/nikola/tmp/qmsg/batch-common/src/main/java/com/example/batch/common/Message.java)

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
- if `action` is missing or blank, the framework maps it to the internal default action key `<<default>>`
- `payload` is deserialized into the batch service payload type before steps are invoked

### Batch service base class

[AbstractBatchService.java](/home/nikola/tmp/qmsg/batch-common/src/main/java/com/example/batch/common/AbstractBatchService.java)

Responsibilities:

- opens the RabbitMQ queue on startup
- starts consuming automatically on startup
- exposes `start()`, `stop()`, and `status()`
- resolves action-to-step-chain mapping from constructor input
- deserializes the incoming message envelope
- creates `BatchContext<P>` and executes the registered steps
- returns `ack` on success
- returns `nack` with requeue on failure

Important implementation detail:

- step routing is driven by `ActionStepTypes`
- `BatchStep.action()` is currently not used for dispatch
- `BatchStep.order()` is currently not used for sorting
- actual execution order is the registration order in `ActionStepTypes`

### Step chain registration

[ActionStepTypes.java](/home/nikola/tmp/qmsg/batch-common/src/main/java/com/example/batch/common/ActionStepTypes.java)

This maps an action name to a list of step classes. The service constructor resolves those classes against injected CDI step beans.

Pattern:

```java
new ActionStepTypes<MyPayload>()
    .onDefault(StepOne.class, StepTwo.class)
    .on("archive", ArchiveStep.class);
```

### Message receiver

[MessageClientReceiver.java](/home/nikola/tmp/qmsg/batch-common/src/main/java/com/example/batch/common/MessageClientReceiver.java)

Responsibilities:

- owns RabbitMQ `Connection` and `Channel`
- declares the queue
- sets `basicQos(1)`
- manages consumer registration and cancellation
- performs `basicAck` or `basicNack(..., requeue=true)` based on handler result

### Batch context

[BatchContext.java](/home/nikola/tmp/qmsg/batch-common/src/main/java/com/example/batch/common/BatchContext.java)

Carries:

- resolved action
- typed payload
- raw message body
- message properties
- receive timestamp
- per-message attribute map for step-to-step state sharing

### Shared REST control resource

[AbstractBatchControlResource.java](/home/nikola/tmp/qmsg/batch-common/src/main/java/com/example/batch/common/AbstractBatchControlResource.java)

Exposes:

- `POST /.../start`
- `POST /.../stop`
- `GET /.../status`

### Shared publisher

[AbstractRabbitBatchPublisher.java](/home/nikola/tmp/qmsg/batch-common/src/main/java/com/example/batch/common/AbstractRabbitBatchPublisher.java)

Responsibilities:

- creates the destination queue if needed
- wraps payload into the shared `Message<P>` envelope
- publishes durable JSON messages to the target queue

## Batch A

### Module purpose

`batch-a` is the sample service with a default action composed of two steps.

### Payload

[BatchAData.java](/home/nikola/tmp/qmsg/batch-a/src/main/java/com/example/batch/a/BatchAData.java)

```json
{
  "id": "A-1",
  "name": "example",
  "amount": 12
}
```

### Queue

[BatchAPublisher.java](/home/nikola/tmp/qmsg/batch-a/src/main/java/com/example/batch/a/BatchAPublisher.java)

- queue name: `queue.BatchAService`

### Action mapping

[BatchAService.java](/home/nikola/tmp/qmsg/batch-a/src/main/java/com/example/batch/a/BatchAService.java)

- default action `<<default>>`
- steps:
  - `BatchAReadPayloadStep`
  - `BatchACompleteStep`

### Steps

[BatchAReadPayloadStep.java](/home/nikola/tmp/qmsg/batch-a/src/main/java/com/example/batch/a/steps/BatchAReadPayloadStep.java)

- reads the typed payload
- logs `id`, `name`, `amount`
- stores `payloadId` into the batch context

[BatchACompleteStep.java](/home/nikola/tmp/qmsg/batch-a/src/main/java/com/example/batch/a/steps/BatchACompleteStep.java)

- reads `payloadId` from the batch context
- logs completion for that payload

### REST endpoints

[BatchAControlResource.java](/home/nikola/tmp/qmsg/batch-a/src/main/java/com/example/batch/a/BatchAControlResource.java)
[BatchAEmitResource.java](/home/nikola/tmp/qmsg/batch-a/src/main/java/com/example/batch/a/BatchAEmitResource.java)

- `POST /batch-a/control/start`
- `POST /batch-a/control/stop`
- `GET /batch-a/control/status`
- `POST /batch-a/messages?action=archive-or-null`

Publishing request body for `batch-a/messages`:

```json
{
  "id": "A-1",
  "name": "example",
  "amount": 12
}
```

Notes:

- the emit endpoint accepts the raw payload body, not the full message envelope
- the publisher wraps it into `{ "action": ..., "payload": ... }`

### Port and RabbitMQ config

[application.yaml](/home/nikola/tmp/qmsg/batch-a/src/main/resources/application.yaml)

- HTTP port: `8080`
- RabbitMQ host: `localhost:5672`
- RabbitMQ user: `guest`

## Batch B

### Module purpose

`batch-b` is the sample service with one default step and one explicit action.

### Payload

[BatchBData.java](/home/nikola/tmp/qmsg/batch-b/src/main/java/com/example/batch/b/BatchBData.java)

```json
{
  "id": "B-1",
  "category": "ops",
  "active": true
}
```

### Queue

[BatchBPublisher.java](/home/nikola/tmp/qmsg/batch-b/src/main/java/com/example/batch/b/BatchBPublisher.java)

- queue name: `queue.BatchBService`

### Action mapping

[BatchBService.java](/home/nikola/tmp/qmsg/batch-b/src/main/java/com/example/batch/b/BatchBService.java)

- default action `<<default>>`: `BatchBDefaultStep`
- explicit action `archive`: `BatchBArchiveStep`

### Steps

[BatchBDefaultStep.java](/home/nikola/tmp/qmsg/batch-b/src/main/java/com/example/batch/b/steps/BatchBDefaultStep.java)

- logs the typed payload fields

[BatchBArchiveStep.java](/home/nikola/tmp/qmsg/batch-b/src/main/java/com/example/batch/b/steps/BatchBArchiveStep.java)

- logs archive processing for the payload id

### REST endpoints

[BatchBControlResource.java](/home/nikola/tmp/qmsg/batch-b/src/main/java/com/example/batch/b/BatchBControlResource.java)
[BatchBEmitResource.java](/home/nikola/tmp/qmsg/batch-b/src/main/java/com/example/batch/b/BatchBEmitResource.java)

- `POST /batch-b/control/start`
- `POST /batch-b/control/stop`
- `GET /batch-b/control/status`
- `POST /batch-b/messages?action=archive-or-null`

### Port and RabbitMQ config

[application.yaml](/home/nikola/tmp/qmsg/batch-b/src/main/resources/application.yaml)

- HTTP port: `8081`
- RabbitMQ host: `localhost:5672`
- RabbitMQ user: `guest`

## How To Add A New Batch Service

1. Create a new Maven module similar to `batch-a` or `batch-b`.
2. Define a typed payload record or class.
3. Implement step beans as `@ApplicationScoped` `BatchStep<P>`.
4. Extend `AbstractBatchService<P>` and register action chains in the constructor using `ActionStepTypes`.
5. Extend `AbstractRabbitBatchPublisher` and return the queue name.
6. Add a control resource by extending `AbstractBatchControlResource<P>`.
7. Add an emit resource that accepts the raw payload and calls `publisher.publish(action, payload)`.
8. Add Quarkus YAML config for HTTP and RabbitMQ settings.

## Example Requests

Start consumer:

```bash
curl -X POST http://localhost:8080/batch-a/control/start
```

Stop consumer:

```bash
curl -X POST http://localhost:8080/batch-a/control/stop
```

Check status:

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

- `BatchStep.action()` is redundant in the current design. Routing is controlled by `ActionStepTypes`.
- `BatchStep.order()` is also redundant right now. Step order comes from constructor registration order.
- The internal default action key is `<<default>>`, while several sample step implementations return `"default"` from `action()`. This does not currently break execution because `action()` is not used for dispatch.
- Consumers auto-start on application startup. Start/stop endpoints still work afterward.
- On processing failure the message is requeued, so poison messages can loop indefinitely without a dead-letter strategy.

## Verification State

The source tree includes compiled `target/classes` output from a prior build, but this repository was not recompiled in the current sandbox session because dependency resolution requires network access.
