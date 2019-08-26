/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.transport.backpressure;

public class ServerTransportRequestLimiterContext {
  private final int partitionId;
  private final boolean isPriority;
  private final long requestId;
  private final long streamId;
  private long startTime;

  public ServerTransportRequestLimiterContext(
      int partitionId, boolean isPriority, long requestId, long streamId) {
    this.partitionId = partitionId;
    this.isPriority = isPriority;
    this.requestId = requestId;
    this.streamId = streamId;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public boolean isPriority() {
    return isPriority;
  }

  public long getRequestId() {
    return requestId;
  }

  public long getStreamId() {
    return streamId;
  }

  public void setStartTime(long currentTimeMillis) {
    this.startTime = currentTimeMillis;
  }

  public long getStartTime() {
    return startTime;
  }
}
