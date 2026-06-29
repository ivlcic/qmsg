package com.example.batch.common;

public record BatchStatus(String queue, boolean consuming, String consumerTag) {
}
