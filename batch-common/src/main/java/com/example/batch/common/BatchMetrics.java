package com.example.batch.common;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class BatchMetrics {
  private static final String ALL = "_all";
  private static final String THROUGHPUT = "batch.execution.throughput";
  private static final String ERRORS = "batch.execution.errors";

  @Inject
  MeterRegistry registry;

  public static class ServiceMetrics {
    private final MeterRegistry registry;
    private final String service;

    private ServiceMetrics(MeterRegistry registry, String service) {
      this.registry = registry;
      this.service = service;
    }

    private void increment(String name, String action, String step) {
      registry.counter(
          name,
          "service", service,
          "action", action,
          "step", step
      ).increment();
    }

    public void recordExecution(String action) {
      increment(THROUGHPUT, ALL, ALL);
      increment(THROUGHPUT, action, ALL);
    }

    public void recordStep(String action, String step) {
      increment(THROUGHPUT, ALL, step);
      increment(THROUGHPUT, action, step);
    }

    public void recordExecutionError(String action) {
      increment(ERRORS, ALL, ALL);
      increment(ERRORS, action, ALL);
    }

    public void recordStepError(String action, String step) {
      increment(ERRORS, ALL, step);
      increment(ERRORS, action, step);
    }
  }

  public ServiceMetrics forService(String service) {
    return new ServiceMetrics(registry, service);
  }
}
