package com.example.batch.common;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 29. 06. 2026.
 */
public record BatchStatus(String queue, BatchServiceState state, String consumerTag) {
}
