/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.transport.backpressure;

import com.netflix.concurrency.limits.Limiter.Listener;
import java.util.Optional;

public class NoLimitsLimiter implements RequestLimiter {

  public static Listener doNothingListener =
      new Listener() {
        @Override
        public void onSuccess() {}

        @Override
        public void onIgnore() {}

        @Override
        public void onDropped() {}
      };

  @Override
  public Optional<Listener> onRequest() {
    return Optional.of(doNothingListener);
  }

  @Override
  public void registerListener(long requestId, Listener listener) {}

  @Override
  public void onResponse(long requestId) {}

  @Override
  public int getLimit() {
    return 0;
  }
}
