# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A small Quarkus-based RabbitMQ batch framework (Java 17, Quarkus 3.35.4) as a Maven multi-module reactor:

- `batch-common` — the reusable framework: message envelope, action routing, step chains, RabbitMQ receiver/emitter, metrics, REST helper interfaces
- `batch-a` — sample service with a single default action of two steps (HTTP port 8080)
- `batch-b` — sample service with multiple actions (`<<default>>`, `archive`, `delete`) and different payload types (HTTP port 8081)

`AGENTS.md` at the repo root is the detailed framework reference (message format, class-by-class responsibilities, metrics tag scheme, how to add a new service, example curl requests). Read it before making framework changes, and keep it in sync when renaming classes or changing REST endpoints — it documents the source by name.

## Build and run

```bash
# Build everything (use a workspace-local repo if ~/.m2 is not writable)
mvn -Dmaven.repo.local=.m2/repository compile

# Run a service in dev mode (requires RabbitMQ at localhost:5672, guest/guest)
mvn -pl batch-a -am -Dmaven.repo.local=.m2/repository quarkus:dev
mvn -pl batch-b -am -Dmaven.repo.local=.m2/repository quarkus:dev
```

Unit tests live in `batch-common/src/test/java` (plain JUnit 5, no CDI container or broker needed). Run with `mvn test`, single test via `mvn -pl <module> test -Dtest=ClassName#methodName`.

## Architecture

Message-driven pipeline. Each concrete service extends `AbstractBatchService` and consumes exactly one RabbitMQ queue named `queue.<ServiceSimpleClassName>`. Messages are a JSON envelope `{"action": "...", "payload": {...}}` (`Message.java`); a missing/blank action maps to the internal key `<<default>>`.

Flow: `Receiver` (implemented by `RabbitMQReceiver`, with its own consuming connection/channel and `basicQos(1)`) delivers raw bytes; the envelope is deserialized (`DefaultDeserializer`, Jackson), the action resolves in the service's `Actions` registry, and its ordered CDI `BatchStep<P>` chain runs against a `BatchContext<P>`. Successful messages are acknowledged; explicit retries are scheduled using the service's immutable `RetryPolicy`; permanent failures and exhausted retries are forwarded to the final DLQ by `RabbitMQDeliveryApprover` before acknowledgement.

Key design decisions to preserve:

- **Transport separation.** `Message`, `EmitterProducer`, `Emitter`, and `Receiver` live in `com.example.batch.common.message`; RabbitMQ implementations and protocol helpers live in its `rabbitmq` subpackage. Framework models such as `BatchResult` and `RetryPolicy` stay in `com.example.batch.common`.
- **Routing lives in the service definition, not in steps.** Concrete services pass only an `Actions` registry to `super(...)` using the fluent API on `BatchService`: static-import `byDefault`/`on`/`with`, e.g. `byDefault(with(BatchAData.class).execute(StepOne.class, StepTwo.class))`. `BatchStep` implementations carry no action/order metadata and are plain `@Dependent` CDI beans.
- **Service-bound sub-objects via `forService(name)`.** `EmitterProducer` (implemented by application-scoped `RabbitMQEmitterProducer`) creates a per-service `Emitter` (`RabbitMQEmitter`). `BatchMetrics` uses the same factory pattern. `AbstractBatchService` depends on the messaging interfaces, not the RabbitMQ implementations.
- **Serializers are passed by callers**, not owned by the emitter — `emit(action, payload, serializer)`.
- **Lifecycle** is an `AtomicReference<BatchServiceState>` (`initializing → initialized → starting → started → stopping → stopped`); services auto-start on Quarkus `StartupEvent`, with retry handled through `Receiver.Controller`.

REST: `BatchStatusResource` interface gives `GET /status`, `/healtz`, `/readyz` (readyz is 503 unless state is `started`); `BatchControlResource` adds `POST /start` and `/stop`. Concrete resources (`BatchAResource` at `/batch-a/messages`, `BatchBResource` at `/batch-b`) implement these and accept the raw payload body (not the envelope) on POST, wrapping it via `service.emit(action, payload)` (async) or `service.execute(context)` (sync). Prometheus metrics at `/q/metrics`: counters `batch.execution.throughput` and `batch.execution.errors` with stable names and `service`/`action`/`step` tags (`_all` for rollups).

## Code conventions

- Define-before-call ordering inside Java files: constructors, helpers, nested classes, and lower-level operations before the methods that call them; for fluent/delegating APIs, the working method before its convenience overloads.
- Lombok is available (`@Slf4j` etc.); keep the existing Javadoc `@author` header style on new classes.
