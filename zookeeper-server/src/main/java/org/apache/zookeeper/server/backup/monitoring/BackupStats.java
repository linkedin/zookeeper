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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains ZK backup related statistics
 */
public class BackupStats {
  private static final Logger LOG = LoggerFactory.getLogger(BackupStats.class);

  private int failedSnapshotIterationCount = 0;
  private long lastSuccessfulSnapshotBackupIterationFinishTime = System.currentTimeMillis();
  private boolean snapshotBackupActive = false;
  private long snapshotIterationDuration = 0L;
  private int numberOfSnapshotFilesBackedUpLastIteration = 0;
  private int failedTxnLogIterationCount = 0;
  private long lastSuccessfulTxnLogBackupIterationFinishTime = System.currentTimeMillis();
  private boolean txnLogBackupActive = false;
  private long txnLogIterationDuration = 0L;

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
    return (System.currentTimeMillis() - lastSuccessfulSnapshotBackupIterationFinishTime) / (60
        * 1000);
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
  public long getNumberOfSnapshotFilesBackedUpLastIteration() {
    return numberOfSnapshotFilesBackedUpLastIteration;
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
    return (System.currentTimeMillis() - lastSuccessfulTxnLogBackupIterationFinishTime) / (60
        * 1000);
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
}