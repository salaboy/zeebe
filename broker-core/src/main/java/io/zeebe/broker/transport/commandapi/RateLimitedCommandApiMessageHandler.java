/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.transport.commandapi;

import com.netflix.concurrency.limits.Limiter.Listener;
import io.zeebe.broker.Loggers;
import io.zeebe.protocol.record.ValueType;
import io.zeebe.protocol.record.intent.Intent;
import io.zeebe.protocol.record.intent.JobIntent;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.transport.ServerOutput;
import io.zeebe.transport.backpressure.RequestLimiter;
import io.zeebe.transport.backpressure.ServerTransportRequestLimiterContext;
import java.util.Optional;
import org.agrona.DirectBuffer;

public class RateLimitedCommandApiMessageHandler extends CommandApiMessageHandler {

  private final RequestLimiter<ServerTransportRequestLimiterContext> limiter;

  public RateLimitedCommandApiMessageHandler(RequestLimiter limiter) {
    super();
    this.limiter = limiter;
  }

  @Override
  public boolean onRequest(
      ServerOutput output,
      RemoteAddress remoteAddress,
      DirectBuffer buffer,
      int offset,
      int length,
      long requestId) {
    final var context = getLimiterContext(buffer, offset, requestId);
    final Optional<Listener> acquire = limiter.onRequest(context);
    if (acquire.isPresent()) {
      return super.onRequest(output, remoteAddress, buffer, offset, length, requestId);
    } else {
      Loggers.TRANSPORT_LOGGER.info("Requests over limit {}, dropping.", limiter.getLimit(context));
      return errorResponseWriter
          .internalError("Backpressure")
          .tryWriteResponse(output, remoteAddress.getStreamId(), requestId);
    }
  }

  private ServerTransportRequestLimiterContext getLimiterContext(
      DirectBuffer buffer, int messageOffset, long requestId) {
    messageHeaderDecoder.wrap(buffer, messageOffset);
    executeCommandRequestDecoder.wrap(
        buffer,
        messageOffset + messageHeaderDecoder.encodedLength(),
        messageHeaderDecoder.blockLength(),
        messageHeaderDecoder.version());

    final ValueType eventType = executeCommandRequestDecoder.valueType();
    final short intent = executeCommandRequestDecoder.intent();
    final Intent commandIntent = Intent.fromProtocolValue(eventType, intent);
    return new ServerTransportRequestLimiterContext(
        executeCommandRequestDecoder.partitionId(),
        commandIntent.equals(JobIntent.COMPLETE),
        requestId);
  }
}
