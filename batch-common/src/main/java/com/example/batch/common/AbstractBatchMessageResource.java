package com.example.batch.common;


public abstract class AbstractBatchMessageResource {

  public <P> Message<P> send(String action, P payload, BatchClientEmitter emitter, Message.Serializer serializer)
      throws Exception {
    Message<P> message = new Message<>();
    message.setAction(action);
    message.setPayload(payload);
    emitter.emit(message, serializer);
    return message;
  }
}
