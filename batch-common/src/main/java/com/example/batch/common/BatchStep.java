package com.example.batch.common;

public interface BatchStep<P> {
    String action();

    int order();

    void execute(BatchContext<P> context) throws Exception;
}
