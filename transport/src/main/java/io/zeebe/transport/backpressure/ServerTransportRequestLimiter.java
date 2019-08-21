/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.transport.backpressure;

import com.netflix.concurrency.limits.limiter.AbstractLimiter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public class ServerTransportRequestLimiter extends AbstractLimiter<Void>
    implements RequestLimiter<Supplier<Boolean>> {

  private final Map<Long, Listener> responseListeners = new HashMap<>();

  protected ServerTransportRequestLimiter(Builder builder) {
    super(builder);
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public Optional<Listener> acquire(Void v) {
    if (getInflight() >= getLimit()) {
      // drop
      return createRejectedListener();
    } else {
      return Optional.of(createListener());
    }
  }

  @Override
  public void registerListener(long requestId, Listener listener) {
    responseListeners.put(requestId, listener);
  }

  @Override
  public void onResponse(long requestId) {
    final Listener listener = responseListeners.remove(requestId);
    if (listener != null) {
      listener.onSuccess();
    }
  }

  @Override
  public Optional<Listener> onRequest(Supplier<Boolean> isPriority) {
    if (isPriority.get()) {
      return Optional.of(createListener());
    } else {
      return acquire(null);
    }
  }

  public static class Builder extends AbstractLimiter.Builder {

    public ServerTransportRequestLimiter build() {
      return new ServerTransportRequestLimiter(this);
    }

    @Override
    protected Builder self() {
      return this;
    }
  }
}
