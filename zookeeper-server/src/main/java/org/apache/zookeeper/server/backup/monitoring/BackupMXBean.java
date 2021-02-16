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

public interface BackupMXBean {
  // Snapshot backup metrics

  /**
   * @return Number of snapshot backup errors since last successful snapshot backup iteration
   */
  int getSnapshotErrorCount();

  /**
   * @return Time passed since last successful snapshot backup iteration
   */
  long getTimeElapsedSinceLastSuccessfulSnapshotIteration();

  /**
   * @return If snapshot backup is currently actively ongoing
   */
  boolean getSnapshotBackupActiveStatus();

  /**
   * @return How long it took to complete the last successful snapshot backup iteration
   */
  long getSnapshotIterationDuration();

  // Transaction log backup metrics

  /**
   * @return Number of txn log backup errors occur after last successful txn log backup iteration
   */
  int getTxnLogErrorCount();

  /**
   * @return Time passed since last successful txn log backup iteration
   */
  long getTimeSinceLastSuccessfulTxnLogIteration();

  /**
   * @return If txn log backup is currently actively ongoing
   */
  boolean getTxnLogBackupActiveStatus();

  /**
   * @return How long it took to complete the last successful txn log backup iteration
   */
  long getTxnLogIterationDuration();
}
