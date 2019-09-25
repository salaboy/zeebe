/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.gateway.impl.job;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.zeebe.gateway.EndpointManager;
import io.zeebe.gateway.Loggers;
import io.zeebe.gateway.ResponseMapper;
import io.zeebe.gateway.impl.broker.BrokerClient;
import io.zeebe.gateway.impl.broker.request.BrokerActivateJobsRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.ActivateJobsResponse;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class ActivateJobsHandler {

  private final Map<String, Integer> jobTypeToNextPartitionId = new HashMap<>();
  private final BrokerClient brokerClient;

  public ActivateJobsHandler(BrokerClient brokerClient) {
    this.brokerClient = brokerClient;
  }

  public void activateJobs(
      int partitionsCount,
      BrokerActivateJobsRequest request,
      int maxJobsToActivate,
      String type,
      Consumer<ActivateJobsResponse> onResponse,
      Consumer<Integer> onCompleted) {
    activateJobs(
        request,
        partitionIdIteratorForType(type, partitionsCount),
        maxJobsToActivate,
        type,
        onResponse,
        onCompleted);
  }

  private void activateJobs(
      BrokerActivateJobsRequest request,
      PartitionIdIterator partitionIdIterator,
      int remainingAmount,
      String jobType,
      Consumer<ActivateJobsResponse> onResponse,
      Consumer<Integer> onCompleted) {
    activateJobs(
        request, partitionIdIterator, remainingAmount, jobType, onResponse, onCompleted, false);
  }

  private void activateJobs(
      BrokerActivateJobsRequest request,
      PartitionIdIterator partitionIdIterator,
      int remainingAmount,
      String jobType,
      Consumer<ActivateJobsResponse> onResponse,
      Consumer<Integer> onCompleted,
      boolean pollPrevPartition) {

    if (remainingAmount > 0 && (pollPrevPartition || partitionIdIterator.hasNext())) {
      final int partitionId =
          pollPrevPartition
              ? partitionIdIterator.getCurrentPartitionId()
              : partitionIdIterator.next();

      // partitions to check and jobs to activate left
      request.setPartitionId(partitionId);
      request.setMaxJobsToActivate(remainingAmount);
      brokerClient.sendRequest(
          request,
          (key, response) -> {
            final ActivateJobsResponse grpcResponse =
                ResponseMapper.toActivateJobsResponse(key, response);
            final int jobsCount = grpcResponse.getJobsCount();
            if (jobsCount > 0) {
              onResponse.accept(grpcResponse);
            }

            activateJobs(
                request,
                partitionIdIterator,
                remainingAmount - jobsCount,
                jobType,
                onResponse,
                onCompleted,
                response.getTruncated());
          },
          error -> {
            final StatusRuntimeException statusRuntimeException =
                EndpointManager.convertThrowable(error);
            if (statusRuntimeException.getStatus().getCode() != Code.RESOURCE_EXHAUSTED) {
              Loggers.GATEWAY_LOGGER.warn(
                  "Failed to activate jobs for type {} from partition {}",
                  jobType,
                  partitionIdIterator.getCurrentPartitionId(),
                  error);
            }
            activateJobs(
                request, partitionIdIterator, remainingAmount, jobType, onResponse, onCompleted);
          });
    } else {
      // enough jobs activated or no more partitions left to check
      jobTypeToNextPartitionId.put(jobType, partitionIdIterator.getCurrentPartitionId());
      onCompleted.accept(remainingAmount);
    }
  }

  private PartitionIdIterator partitionIdIteratorForType(String jobType, int partitionsCount) {
    final Integer nextPartitionId = jobTypeToNextPartitionId.computeIfAbsent(jobType, t -> 0);
    return new PartitionIdIterator(nextPartitionId, partitionsCount);
  }
}
