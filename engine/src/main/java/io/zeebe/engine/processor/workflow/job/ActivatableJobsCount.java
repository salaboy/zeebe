/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processor.workflow.job;

import io.zeebe.engine.Loggers;
import io.zeebe.engine.processor.ReadonlyProcessingContext;
import io.zeebe.engine.processor.StreamProcessorLifecycleAware;
import io.zeebe.engine.state.instance.JobState;
import io.zeebe.util.buffer.BufferUtil;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;

public class ActivatableJobsCount
    implements StreamProcessorLifecycleAware, JobTypeAvailibilityListener {

  private static final Logger LOG = Loggers.STREAM_PROCESSING;
  private final Map<String, Integer> availableJobCount = new HashMap<>();
  private final JobState state;
  private final Consumer<String> jobsAvailableCallback;

  public ActivatableJobsCount(JobState jobState, Consumer<String> jobsAvailableCallback) {
    this.state = jobState;
    this.jobsAvailableCallback = jobsAvailableCallback;
  }

  @Override
  public void onRecovered(ReadonlyProcessingContext context) {
    getCurrentAvailableJobs();
    state.addJobTypeAvailabilityListener(this);
  }

  @Override
  public void onJobCreated(DirectBuffer jobType) {
    final String type = BufferUtil.bufferAsString(jobType);
    availableJobCount.compute(type, (t, count) -> incrementAndNotify(type, count));
  }

  @Override
  public void onJobNotActivatable(DirectBuffer jobType) {
    final String type = BufferUtil.bufferAsString(jobType);
    availableJobCount.compute(type, (t, count) -> decrementAndDelete(count));
  }

  private void getCurrentAvailableJobs() {
    state.forEachActivatableJobEntry(
        type ->
            availableJobCount.compute(
                BufferUtil.bufferAsString(type), (t, count) -> count == null ? 1 : count + 1));
  }

  private Integer incrementAndNotify(String type, Integer currentCount) {
    if (currentCount == null || currentCount == 0) {
      jobsAvailableCallback.accept(type);
      return 1;
    }
    return currentCount + 1;
  }

  private Integer decrementAndDelete(Integer currentCount) {
    return currentCount == 1 ? null : currentCount - 1;
  }
}
