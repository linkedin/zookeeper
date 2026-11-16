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
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains ZK backup related statistics
 */
public class BackupStats {
  private static final Logger LOG = LoggerFactory.getLogger(BackupStats.class);

  /**
   * Files in the backup status directory holding the finish time of the last successful iteration.
   */
  static final String LAST_SUCCESSFUL_SNAPSHOT_ITERATION_FILENAME = "zkBackupLastSuccessfulSnapshotIteration";
  static final String LAST_SUCCESSFUL_TXNLOG_ITERATION_FILENAME = "zkBackupLastSuccessfulTxnLogIteration";

  private final LongSupplier clock;
  private final PersistedTimestamp persistedSnapshotSuccess;
  private final PersistedTimestamp persistedTxnLogSuccess;

  /*
    Snapshot backup metric declarations
   */
  private int failedSnapshotIterationCount = 0;
  private long lastSuccessfulSnapshotBackupIterationFinishTime;
  private volatile long lastSuccessfulSnapshotIterationFinishTimeAcrossRestarts;
  private boolean snapshotBackupActive = false;
  private long snapshotIterationDuration = 0L;
  private int numberOfSnapshotFilesBackedUpLastIteration = 0;
  private int numberOfSnapshotFilesBackedUpThisIteration = 0;
  private long lastSnapshotBackupIterationStartTime;

  /*
    TxLog backup metric declarations
   */
  private int failedTxnLogIterationCount = 0;
  private long lastSuccessfulTxnLogBackupIterationFinishTime;
  private volatile long lastSuccessfulTxnLogIterationFinishTimeAcrossRestarts;
  private boolean txnLogBackupActive = false;
  private long txnLogIterationDuration = 0L;
  private long lastTxnLogBackupIterationStartTime;

  /**
   * Create stats that start over with the process; nothing is persisted.
   */
  public BackupStats() {
    this(null, System::currentTimeMillis);
  }

  /**
   * Create stats that keep the last successful iteration times in the backup status directory, so
   * the "AcrossRestarts" gauges continue after a restart.
   * @param statusDir the backup status directory; null disables persistence
   */
  public BackupStats(File statusDir) {
    this(statusDir, System::currentTimeMillis);
  }

  BackupStats(File statusDir, LongSupplier clock) {
    this.clock = clock;
    long now = clock.getAsLong();
    lastSuccessfulSnapshotBackupIterationFinishTime = now;
    lastSnapshotBackupIterationStartTime = now;
    lastSuccessfulTxnLogBackupIterationFinishTime = now;
    lastTxnLogBackupIterationStartTime = now;
    persistedSnapshotSuccess = new PersistedTimestamp(statusDir, LAST_SUCCESSFUL_SNAPSHOT_ITERATION_FILENAME);
    persistedTxnLogSuccess = new PersistedTimestamp(statusDir, LAST_SUCCESSFUL_TXNLOG_ITERATION_FILENAME);
    lastSuccessfulSnapshotIterationFinishTimeAcrossRestarts =
        Math.min(now, persistedSnapshotSuccess.readOrDefault(now));
    lastSuccessfulTxnLogIterationFinishTimeAcrossRestarts =
        Math.min(now, persistedTxnLogSuccess.readOrDefault(now));
  }

  // Snapshot backup metrics

  /**
   * Counter
   * For example: if backup iteration A fails, the number is 1; if next backup iteration B succeeds, the number is reset to 0.
   * If A fails, the number is 1; if then B fails too, the number is incremented to 2.
   * @return Number of consecutive snapshot backup errors since last successful snapshot backup iteration
   */
  public int getNumConsecutiveFailedSnapshotIterations() {
    return failedSnapshotIterationCount;
  }

  /**
   * Counter
   * @return Time passed (minutes) since last successful snapshot backup iteration
   */
  public long getMinutesSinceLastSuccessfulSnapshotIteration() {
    return (clock.getAsLong() - lastSuccessfulSnapshotBackupIterationFinishTime) / (60
        * 1000);
  }

  /**
   * Gauge
   * @return Time passed (minutes) since last successful snapshot backup iteration, including
   *         iterations that finished before the latest server restart
   */
  public long getMinutesSinceLastSuccessfulSnapshotIterationAcrossRestarts() {
    return (clock.getAsLong() - lastSuccessfulSnapshotIterationFinishTimeAcrossRestarts) / (60 * 1000);
  }

  /**
   * Gauge
   * @return If snapshot backup is currently actively ongoing
   */
  public boolean getSnapshotBackupActiveStatus() {
    return snapshotBackupActive;
  }

  /**
   * Gauge
   * @return How long it took to complete the last snapshot backup iteration
   */
  public long getSnapshotIterationDuration() {
    return snapshotIterationDuration;
  }

  /**
   * Gauge
   * @return Number of snapshot files that were backed up to backup storage in last snapshot backup iteration
   */
  public int getNumberOfSnapshotFilesBackedUpLastIteration() {
    return numberOfSnapshotFilesBackedUpLastIteration;
  }

  /**
   * Record the status and timestamp when a snapshot backup iteration starts
   */
  public void setSnapshotBackupIterationStart() {
    lastSnapshotBackupIterationStartTime = clock.getAsLong();
    snapshotBackupActive = true;
    numberOfSnapshotFilesBackedUpThisIteration = 0;
  }

  /**
   *  Record the status and timestamp when a snapshot backup iteration finishes
   * @param errorFree If this iteration finishes without error
   */
  public void setSnapshotBackupIterationDone(boolean errorFree) {
    long finishTime = clock.getAsLong();
    snapshotIterationDuration = finishTime - lastSnapshotBackupIterationStartTime;
    snapshotBackupActive = false;
    numberOfSnapshotFilesBackedUpLastIteration = numberOfSnapshotFilesBackedUpThisIteration;
    numberOfSnapshotFilesBackedUpThisIteration = 0;
    if (errorFree) {
      lastSuccessfulSnapshotBackupIterationFinishTime = finishTime;
      lastSuccessfulSnapshotIterationFinishTimeAcrossRestarts = finishTime;
      persistedSnapshotSuccess.write(finishTime);
      failedSnapshotIterationCount = 0;
    } else {
      failedSnapshotIterationCount++;
    }
  }

  /**
   * To be called during snapshot backup iteration every time one more snapshot file is backed up
   * A helper to keep track value for "numberOfSnapshotFilesBackedUpLastIteration" metric
   */
  public void incrementNumberOfSnapshotFilesBackedUpThisIteration() {
    numberOfSnapshotFilesBackedUpThisIteration++;
  }

  // Transaction log backup metrics

  /**
   * Counter
   * For example: if backup iteration A fails, the number is 1; if next backup iteration B succeeds, the number is reset to 0.
   * If A fails, the number is 1; if then B fails too, the number is incremented to 2.
   * @return Number of consecutive txn log backup errors since last successful txn log backup iteration
   */
  public int getNumConsecutiveFailedTxnLogIterations() {
    return failedTxnLogIterationCount;
  }

  /**
   * Counter
   * @return Time passed (minutes) since last successful txn log backup iteration
   */
  public long getMinutesSinceLastSuccessfulTxnLogIteration() {
    return (clock.getAsLong() - lastSuccessfulTxnLogBackupIterationFinishTime) / (60
        * 1000);
  }

  /**
   * Gauge
   * @return Time passed (minutes) since last successful txn log backup iteration, including
   *         iterations that finished before the latest server restart
   */
  public long getMinutesSinceLastSuccessfulTxnLogIterationAcrossRestarts() {
    return (clock.getAsLong() - lastSuccessfulTxnLogIterationFinishTimeAcrossRestarts) / (60 * 1000);
  }

  /**
   * Gauge
   * @return If txn log backup is currently actively ongoing
   */
  public boolean getTxnLogBackupActiveStatus() {
    return txnLogBackupActive;
  }

  /**
   * Gauge
   * @return How long it took to complete the last txn log backup iteration
   */
  public long getTxnLogIterationDuration() {
    return txnLogIterationDuration;
  }

  /**
   * Record the status and timestamp when a txn log backup iteration starts
   */
  public void setTxnLogBackupIterationStart() {
    lastTxnLogBackupIterationStartTime = clock.getAsLong();
    txnLogBackupActive = true;
  }

  /**
   *  Record the status and timestamp when a txn log backup iteration finishes
   * @param errorFree If this iteration finishes without error
   */
  public void setTxnLogBackupIterationDone(boolean errorFree) {
    long finishTime = clock.getAsLong();
    txnLogBackupActive = false;
    txnLogIterationDuration = finishTime - lastTxnLogBackupIterationStartTime;
    if (errorFree) {
      lastSuccessfulTxnLogBackupIterationFinishTime = finishTime;
      lastSuccessfulTxnLogIterationFinishTimeAcrossRestarts = finishTime;
      persistedTxnLogSuccess.write(finishTime);
      failedTxnLogIterationCount = 0;
    } else {
      failedTxnLogIterationCount++;
    }
  }
}