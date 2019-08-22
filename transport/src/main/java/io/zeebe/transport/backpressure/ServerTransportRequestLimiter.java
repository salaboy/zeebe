/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.transport.backpressure;

import com.netflix.concurrency.limits.limiter.AbstractLimiter;
import java.util.Optional;

public class ServerTransportRequestLimiter
    extends AbstractLimiter<ServerTransportRequestLimiterContext> {

  protected ServerTransportRequestLimiter(Builder builder) {
    super(builder);
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public Optional<Listener> acquire(ServerTransportRequestLimiterContext context) {
    if (getInflight() >= getLimit() && !context.isPriority()) {
      // drop
      return createRejectedListener();
    } else {
      return Optional.of(createListener());
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
