/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.transport.backpressure;

import com.netflix.concurrency.limits.Limiter.Listener;
import io.zeebe.transport.backpressure.ServerTransportRequestLimiter.Builder;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class PartitionedServerTransportRequestLimter
    implements RequestLimiter<ServerTransportRequestLimiterContext> {

  private final Map<Long, Listener> responseListeners = new HashMap<>();
  private final Map<Integer, ServerTransportRequestLimiter> partitionLimiters;
  private final Builder partitionLimiterBuilder;

  public PartitionedServerTransportRequestLimter(Builder partitionLimiterBuilder) {
    this.partitionLimiterBuilder = partitionLimiterBuilder;
    this.partitionLimiters = new HashMap<>();
  }

  @Override
  public void onResponse(long requestId) {
    final Listener listener = responseListeners.remove(requestId);
    if (listener != null) {
      listener.onSuccess();
    }
  }

  @Override
  public int getLimit(ServerTransportRequestLimiterContext context) {
    return partitionLimiters.get(context.getPartitionId()).getLimit();
  }

  @Override
  public Optional<Listener> onRequest(ServerTransportRequestLimiterContext context) {
    final Optional<Listener> listener =
        partitionLimiters
            .computeIfAbsent(context.getPartitionId(), p -> partitionLimiterBuilder.build())
            .acquire(context);
    listener.ifPresent(l -> registerListener(context.getRequestId(), l));
    return listener;
  }

  private void registerListener(long requestId, Listener l) {
    responseListeners.put(requestId, l);
  }
}
