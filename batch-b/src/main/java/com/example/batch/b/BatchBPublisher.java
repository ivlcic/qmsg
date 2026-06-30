package com.example.batch.b;

import com.example.batch.common.BatchClientEmitter;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class BatchBPublisher extends BatchClientEmitter {
  public BatchBPublisher() {
    super(BatchBService.class.getSimpleName());
  }
}
