package com.example.batch.a;

import com.example.batch.common.BatchClientEmitter;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class BatchAPublisher extends BatchClientEmitter {
  public BatchAPublisher() {
    super(BatchAService.class.getSimpleName());
  }
}
