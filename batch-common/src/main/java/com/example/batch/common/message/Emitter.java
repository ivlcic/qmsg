package com.example.batch.common.message;

/**
 * @author Nikola Ivacic <nikola.ivacic@dropchop.com> on 05. 11. 2026.
 */
public interface Emitter extends AutoCloseable {
  <P> void emit(Message<P> message, Message.Serializer serializer);

  <P> Message<P> emit(String action, P payload, Message.Serializer serializer) throws Exception;

  @Override
  void close();
}
