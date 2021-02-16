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

  private int snapshotErrorCount = 0;
  private long lastSuccessfulSnapshotBackupIterationFinishTime = System.currentTimeMillis();
  private boolean snapshotBackupActive = false;
  private long snapshotIterationDuration = 0L;
  private int numberOfSnapshotBackupFilesCreatedLastIteration = 0;
  private int txnLogErrorCount = 0;
  private long lastSuccessfulTxnLogBackupIterationFinishTime = System.currentTimeMillis();
  private boolean txnLogBackupActive = false;
  private long txnLogIterationDuration = 0L;

  // Snapshot backup metrics

  /**
   * @return Number of snapshot backup errors occur since last successful snapshot backup iteration
   */
  public int getSnapshotErrorCount() {
    return snapshotErrorCount;
  }

  /**
   * @return Time passed (minutes) since last successful snapshot backup iteration
   */
  public long getMinutesSinceLastSuccessfulSnapshotIteration() {
    return (System.currentTimeMillis() - lastSuccessfulSnapshotBackupIterationFinishTime) / (60 * 1000);
  }

  /**
   * @return If snapshot backup is currently actively ongoing
   */
  public boolean getSnapshotBackupActiveStatus() {
    return snapshotBackupActive;
  }

  /**
   * @return How long it took to complete the last successful snapshot backup iteration
   */
  public long getSnapshotIterationDuration() {
    return snapshotIterationDuration;
  }

  /**
   * @return Number of backup files created in last snapshot backup iteration
   */
  public long getNumberOfSnapshotBackupFilesCreatedLastIteration() {
    return numberOfSnapshotBackupFilesCreatedLastIteration;
  }

  // Transaction log backup metrics

  /**
   * @return Number of txn log backup errors after last successful txn log backup iteration
   */
  public int getTxnLogErrorCount() {
    return txnLogErrorCount;
  }

  /**
   * @return Time passed (minutes) since last successful txn log backup iteration
   */
  public long getMinutesSinceLastSuccessfulTxnLogIteration() {
    return (System.currentTimeMillis() - lastSuccessfulTxnLogBackupIterationFinishTime) / (60 * 1000);
  }

  /**
   * @return If txn log backup is currently actively ongoing
   */
  public boolean getTxnLogBackupActiveStatus() {
    return txnLogBackupActive;
  }

  /**
   * @return How long it took to complete the last successful txn log backup iteration
   */
  public long getTxnLogIterationDuration() {
    return txnLogIterationDuration;
  }
}