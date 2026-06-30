package com.example.batch.common;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 03. 06. 2026.
 */
public class Steps<P> implements Iterable<BatchStep<P>> {

  private final List<BatchStep<P>> steps = new LinkedList<>();
  private final List<Class<? extends BatchStep<P>>> types;
  private final Class<P> payloadType;

  public Steps(Collection<Class<? extends BatchStep<P>>> types) {
    this.types = new LinkedList<>(types);
    this.payloadType = resolvePayloadType(this.types);
  }

  void add(BatchStep<?> step) {
    //noinspection unchecked
    this.steps.add((BatchStep<P>) step);
  }

  public List<Class<? extends BatchStep<P>>> types() {
    return types;
  }

  public Class<P> payloadType() {
    return payloadType;
  }

  public boolean isEmpty() {
    return steps.isEmpty();
  }

  @Override
  public Iterator<BatchStep<P>> iterator() {
    return steps.iterator();
  }

  private Class<P> resolvePayloadType(List<Class<? extends BatchStep<P>>> stepTypes) {
    Class<?> payloadType = null;
    for (Class<? extends BatchStep<P>> stepType : stepTypes) {
      Class<?> currentPayloadType = findPayloadType(stepType);
      if (payloadType != null && !payloadType.equals(currentPayloadType)) {
        throw new IllegalArgumentException(
            "All steps for one action must use the same payload type. Found "
                + payloadType.getName() + " and " + currentPayloadType.getName()
        );
      }
      payloadType = currentPayloadType;
    }
    if (payloadType == null) {
      throw new IllegalArgumentException("At least one step type is required");
    }
    @SuppressWarnings("unchecked")
    Class<P> typedPayloadType = (Class<P>) payloadType;
    return typedPayloadType;
  }

  private static Class<?> findPayloadType(Class<?> stepType) {
    Class<?> current = stepType;
    while (current != null && !Object.class.equals(current)) {
      for (Type interfaceType : current.getGenericInterfaces()) {
        Class<?> payloadType = payloadTypeFrom(interfaceType);
        if (payloadType != null) {
          return payloadType;
        }
      }

      Type superType = current.getGenericSuperclass();
      Class<?> payloadType = payloadTypeFrom(superType);
      if (payloadType != null) {
        return payloadType;
      }
      current = rawClass(superType);
    }

    throw new IllegalArgumentException(
        "Cannot determine BatchStep payload type for " + stepType.getName()
    );
  }

  private static Class<?> payloadTypeFrom(Type type) {
    if (type instanceof ParameterizedType parameterizedType) {
      Type rawType = parameterizedType.getRawType();
      if (rawType instanceof Class<?> rawClass && BatchStep.class.isAssignableFrom(rawClass)) {
        return typeArgumentClass(parameterizedType.getActualTypeArguments()[0]);
      }
      Class<?> nestedPayloadType = payloadTypeFrom(rawType);
      if (nestedPayloadType != null) {
        return nestedPayloadType;
      }
    }
    if (type instanceof Class<?> typeClass && !BatchStep.class.equals(typeClass)) {
      try {
        return findPayloadType(typeClass);
      } catch (IllegalArgumentException ignored) {
        return null;
      }
    }
    return null;
  }

  private static Class<?> typeArgumentClass(Type type) {
    if (type instanceof Class<?> typeClass) {
      return typeClass;
    }
    if (type instanceof ParameterizedType parameterizedType
        && parameterizedType.getRawType() instanceof Class<?> rawClass) {
      return rawClass;
    }
    throw new IllegalArgumentException("Unsupported BatchStep payload type " + type.getTypeName());
  }

  private static Class<?> rawClass(Type type) {
    if (type instanceof Class<?> typeClass) {
      return typeClass;
    }
    if (type instanceof ParameterizedType parameterizedType
        && parameterizedType.getRawType() instanceof Class<?> rawClass) {
      return rawClass;
    }
    return null;
  }
}
