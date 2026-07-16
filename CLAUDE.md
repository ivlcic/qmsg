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

Flow: `BatchReceiver` (owns Connection/Channel, `basicQos(1)`) delivers raw bytes → envelope is deserialized (`DefaultDeserializer`, Jackson) → the action name resolves an `Action` in the service's `Actions` registry → the action's ordered chain of `BatchStep<P>` CDI beans runs against a `BatchContext<P>` (typed payload + attribute map for step-to-step state) → success acks, failure nacks with requeue (poison messages loop; no dead-letter yet).

Key design decisions to preserve:

- **Routing lives in the service definition, not in steps.** Concrete services pass only an `Actions` registry to `super(...)` using the fluent API on `BatchService`: static-import `byDefault`/`on`/`with`, e.g. `byDefault(with(BatchAData.class).execute(StepOne.class, StepTwo.class))`. `BatchStep` implementations carry no action/order metadata and are plain `@Dependent` CDI beans.
- **Service-bound sub-objects via `forService(name)`.** `BatchEmitter`, `BatchMetrics` (and the emitter/metrics fields in `AbstractBatchService`) follow this pattern: an `@ApplicationScoped` factory whose `forService(...)` returns a per-service instance. Keep new cross-cutting facilities in this style.
- **Serializers are passed by callers**, not owned by the emitter — `emit(action, payload, serializer)`.
- **Lifecycle** is an `AtomicReference<BatchServiceState>` (`initializing → initialized → starting → started → stopping → stopped`); services auto-start on Quarkus `StartupEvent`, with retry handled through `BatchReceiver.Controller`.

REST: `BatchStatusResource` interface gives `GET /status`, `/healtz`, `/readyz` (readyz is 503 unless state is `started`); `BatchControlResource` adds `POST /start` and `/stop`. Concrete resources (`BatchAResource` at `/batch-a/messages`, `BatchBResource` at `/batch-b`) implement these and accept the raw payload body (not the envelope) on POST, wrapping it via `service.emit(action, payload)` (async) or `service.execute(context)` (sync). Prometheus metrics at `/q/metrics`: counters `batch.execution.throughput` and `batch.execution.errors` with stable names and `service`/`action`/`step` tags (`_all` for rollups).

## Code conventions

- Define-before-call ordering inside Java files: constructors, helpers, nested classes, and lower-level operations before the methods that call them; for fluent/delegating APIs, the working method before its convenience overloads.
- Lombok is available (`@Slf4j` etc.); keep the existing Javadoc `@author` header style on new classes.
