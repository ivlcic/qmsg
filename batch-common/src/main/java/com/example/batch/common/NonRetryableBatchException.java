package com.example.batch.common;

public class NonRetryableBatchException extends RuntimeException {
    public NonRetryableBatchException(String message) {
        super(message);
    }

    public NonRetryableBatchException(String message, Throwable cause) {
        super(message, cause);
    }
}
