/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.backup.monitoring;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * TimetableBackupStats contains fields that represent metrics for timetable backup.
 */
public class TimetableBackupStats {
  /**
   * File in the backup status directory holding the finish time of the last successful iteration.
   */
  static final String LAST_SUCCESSFUL_TIMETABLE_ITERATION_FILENAME = "zkBackupLastSuccessfulTimetableIteration";

  private final LongSupplier clock;
  private final PersistedTimestamp persistedTimetableSuccess;

  /*
    Timetable backup metric declarations
  */
  private int failedTimetableIterationCount = 0;
  private boolean timetableBackupActive = false;
  private long lastSuccessfulTimetableBackupIterationFinishTime;
  private volatile long lastSuccessfulTimetableIterationFinishTimeAcrossRestarts;
  private long timetableIterationDuration = 0L;
  private long lastTimetableBackupIterationStartTime;

  /**
   * Create stats that start over with the process; nothing is persisted.
   */
  public TimetableBackupStats() {
    this(null, System::currentTimeMillis);
  }

  /**
   * Create stats that keep the last successful iteration time in the backup status directory, so
   * the "AcrossRestarts" gauge continues after a restart.
   * @param statusDir the backup status directory; null disables persistence
   */
  public TimetableBackupStats(File statusDir) {
    this(statusDir, System::currentTimeMillis);
  }

  TimetableBackupStats(File statusDir, LongSupplier clock) {
    this.clock = clock;
    long now = clock.getAsLong();
    lastSuccessfulTimetableBackupIterationFinishTime = now;
    lastTimetableBackupIterationStartTime = now;
    persistedTimetableSuccess = new PersistedTimestamp(statusDir, LAST_SUCCESSFUL_TIMETABLE_ITERATION_FILENAME);
    lastSuccessfulTimetableIterationFinishTimeAcrossRestarts =
        Math.min(now, persistedTimetableSuccess.readOrDefault(now));
  }

  // Timetable backup metrics

  /**
   * Counter
   * For example: if backup iteration A fails, the number is 1; if next backup iteration B succeeds,
   * the number is reset to 0.
   * If A fails, the number is 1; if then the next iteration B fails again, the number is
   * incremented to 2.
   * @return Number of consecutive timetable backup errors since last successful timetable backup
   *         iteration
   */
  public int getNumConsecutiveFailedTimetableIterations() {
    return failedTimetableIterationCount;
  }

  /**
   * Counter
   * @return Time passed (minutes) since last successful timetable backup iteration
   */
  public long getMinutesSinceLastSuccessfulTimetableIteration() {
    return TimeUnit.MILLISECONDS
        .toMinutes(clock.getAsLong() - lastSuccessfulTimetableBackupIterationFinishTime);
  }

  /**
   * Gauge
   * @return Time passed (minutes) since last successful timetable backup iteration, including
   *         iterations that finished before the latest server restart
   */
  public long getMinutesSinceLastSuccessfulTimetableIterationAcrossRestarts() {
    return TimeUnit.MILLISECONDS
        .toMinutes(clock.getAsLong() - lastSuccessfulTimetableIterationFinishTimeAcrossRestarts);
  }

  /**
   * Gauge
   * @return true if timetable backup is currently in progress
   */
  public boolean getTimetableBackupActiveStatus() {
    return timetableBackupActive;
  }

  /**
   * Gauge
   * @return How long it took to complete the last timetable backup iteration
   */
  public long getTimetableIterationDuration() {
    return timetableIterationDuration;
  }

  /**
   * Record the status and timestamp when a timetable backup iteration starts
   */
  public void setTimetableBackupIterationStart() {
    lastTimetableBackupIterationStartTime = clock.getAsLong();
    timetableBackupActive = true;
  }

  /**
   * Record the status and timestamp when a timetable backup iteration finishes.
   * @param errorFree If this iteration finishes without error
   */
  public void setTimetableBackupIterationDone(boolean errorFree) {
    long finishTime = clock.getAsLong();
    timetableBackupActive = false;
    timetableIterationDuration = finishTime - lastTimetableBackupIterationStartTime;
    if (errorFree) {
      lastSuccessfulTimetableBackupIterationFinishTime = finishTime;
      lastSuccessfulTimetableIterationFinishTimeAcrossRestarts = finishTime;
      persistedTimetableSuccess.write(finishTime);
      failedTimetableIterationCount = 0;
    } else {
      failedTimetableIterationCount++;
    }
  }
}
