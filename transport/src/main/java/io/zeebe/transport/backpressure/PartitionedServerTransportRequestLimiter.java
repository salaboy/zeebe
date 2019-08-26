/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.transport.backpressure;

import com.netflix.concurrency.limits.Limiter.Listener;
import io.zeebe.transport.Loggers;
import io.zeebe.transport.backpressure.ServerTransportRequestLimiter.Builder;
import io.zeebe.util.sched.clock.ActorClock;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.slf4j.LoggerFactory;

public class PartitionedServerTransportRequestLimiter
    implements RequestLimiter<ServerTransportRequestLimiterContext> {

  private static final long TIMEOUT = Duration.ofSeconds(5).toMillis();
  private final ReentrantLock lock = new ReentrantLock(); // just to make it easy for now

  private final Map<Long, Map<Long, Listener>> responseListeners = new HashMap<>();
  private final Map<Long, Map<Long, ServerTransportRequestLimiterContext>> listenerTimeouts =
      new HashMap();
  private final Map<Integer, ServerTransportRequestLimiter> partitionLimiters;
  private final Builder partitionLimiterBuilder;

  public PartitionedServerTransportRequestLimiter(Builder partitionLimiterBuilder) {
    this.partitionLimiterBuilder = partitionLimiterBuilder;
    this.partitionLimiters = new HashMap<>();
  }

  @Override
  public void onResponse(long requestId, long streamId) {
    lock.lock();
    try {
      final Listener listener = responseListeners.get(streamId).remove(requestId);
      if (listener != null) {
        timeout(streamId, requestId);
        listener.onSuccess();
      }
    } catch (Exception e) {
      LoggerFactory.getLogger("FINDME:").warn("Exception at limiter.response", e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public int getLimit(ServerTransportRequestLimiterContext context) {
    return partitionLimiters.get(context.getPartitionId()).getLimit();
  }

  @Override
  public Optional<Listener> onRequest(ServerTransportRequestLimiterContext context) {
    Optional<Listener> listener = Optional.empty();
    lock.lock();
    try {
      listener =
          partitionLimiters
              .computeIfAbsent(context.getPartitionId(), p -> partitionLimiterBuilder.build())
              .acquire(context);
      listener.ifPresent(
          l -> {
            registerListener(context.getRequestId(), context.getStreamId(), l, context);
            timeoutListeners(context.getStreamId());
          });
    } catch (Exception e) {
      LoggerFactory.getLogger("FINDME:").warn("Exception at limiter.request", e);
    } finally {
      lock.unlock();
    }
    return listener;
  }

  @Override
  public int getInflight(ServerTransportRequestLimiterContext context) {
    return partitionLimiters.get(context.getPartitionId()).getInflight();
  }

  private void timeoutListeners(long streamId) {
    final long currentTime = ActorClock.currentTimeMillis();
    final List<ServerTransportRequestLimiterContext> timedoutRequests =
        listenerTimeouts.get(streamId).values().stream()
            .filter(context -> currentTime - context.getStartTime() > TIMEOUT)
            .collect(Collectors.toList());

    timedoutRequests.forEach(
        context -> {
          final Map<Long, Listener> streams = responseListeners.get(context.getStreamId());
          final Listener listener = streams.remove(context.getRequestId());
          if (listener != null) {
            listener.onIgnore();
          }
          listenerTimeouts.get(context.getStreamId()).remove(context.getRequestId());
          Loggers.TRANSPORT_LOGGER.warn(
              "FINDME: limiter listener timeout {} {}",
              context.getStreamId(),
              context.getRequestId());
        });
  }

  private void timeout(long streamId, long requestId) {
    listenerTimeouts.get(streamId).remove(requestId);
  }

  private void registerListener(
      long requestId,
      long streamId,
      Listener listener,
      ServerTransportRequestLimiterContext context) {
    responseListeners
        .computeIfAbsent(streamId, s -> new ConcurrentHashMap<>())
        .put(requestId, listener);
    context.setStartTime(ActorClock.currentTimeMillis());
    listenerTimeouts
        .computeIfAbsent(streamId, s -> new ConcurrentHashMap<>())
        .put(requestId, context);
  }
}
