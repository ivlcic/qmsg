package com.example.batch.common;

public interface BatchStep<P> {
    void execute(BatchContext<P> context) throws Exception;
}
