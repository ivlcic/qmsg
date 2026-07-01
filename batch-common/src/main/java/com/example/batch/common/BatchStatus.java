package com.example.batch.common;

public record BatchStatus(String queue, BatchServiceState state, String consumerTag) {
}
