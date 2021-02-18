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

/**
 * ZK backup MBean
 */
public interface BackupMXBean {
  // Snapshot backup metrics

  /**
   * Counter
   * @return Number of snapshot backup errors since last successful snapshot backup iteration
   */
  int getSuccessiveSnapshotIterationErrorCount();

  /**
   * Counter
   * @return Time passed (minutes) since last successful snapshot backup iteration
   */
  long getMinutesSinceLastSuccessfulSnapshotIteration();

  /**
   * Gauge
   * @return If snapshot backup is currently actively ongoing
   */
  boolean getSnapshotBackupActiveStatus();

  /**
   * Gauge
   * @return How long it took to complete the last snapshot backup iteration
   */
  long getLastSnapshotIterationDuration();

  /**
   * Gauge
   * @return Number of snapshot files that were backed up to backup storage in last snapshot backup iteration
   */
  long getNumberOfSnapshotFilesBackedUpLastIteration();

  // Transaction log backup metrics

  /**
   * Counter
   * @return Number of txn log backup errors occur after last successful txn log backup iteration
   */
  int getSuccessiveTxnLogIterationErrorCount();

  /**
   * Counter
   * @return Time passed (minutes) since last successful txn log backup iteration
   */
  long getMinutesSinceLastSuccessfulTxnLogIteration();

  /**
   * Gauge
   * @return If txn log backup is currently actively ongoing
   */
  boolean getTxnLogBackupActiveStatus();

  /**
   * Gauge
   * @return How long it took to complete the last txn log backup iteration
   */
  long getLastTxnLogIterationDuration();
}
