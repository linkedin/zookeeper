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

import org.apache.zookeeper.jmx.ZKMBeanInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements ZK backup MBean
 */
public class BackupBean implements ZKMBeanInfo, BackupMXBean {
  private static final Logger LOG = LoggerFactory.getLogger(BackupBean.class);

  private final BackupStats backupStats;
  private final String name;

  public BackupBean(BackupStats backupStats, String namespace, long serverId) {
    this.backupStats = backupStats;
    name = "Backup_" + namespace + ".server" + serverId;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public boolean isHidden() {
    return false;
  }

  // Snapshot backup metrics
  @Override
  public int getSnapshotErrorCount() {
    return backupStats.getSnapshotErrorCount();
  }

  @Override
  public long getTimeElapsedSinceLastSuccessfulSnapshotIteration() {
    return backupStats.getTimeElapsedSinceLastSuccessfulSnapshotIteration();
  }

  @Override
  public boolean getSnapshotBackupActiveStatus() {
    return backupStats.getSnapshotBackupActiveStatus();
  }

  /**
   * @return How long it took to complete the last successful snapshot backup iteration
   */
  public long getSnapshotIterationDuration() {
    return backupStats.getSnapshotIterationDuration();
  }

  // Transaction log backup metrics
  @Override
  public int getTxnLogErrorCount() {
    return backupStats.getTxnLogErrorCount();
  }

  @Override
  public long getTimeSinceLastSuccessfulTxnLogIteration() {
    return backupStats.getTimeSinceLastSuccessfulTxnLogIteration();
  }

  @Override
  public boolean getTxnLogBackupActiveStatus() {
    return backupStats.getTxnLogBackupActiveStatus();
  }

  @Override
  public long getTxnLogIterationDuration() {
    return backupStats.getTxnLogIterationDuration();
  }
}
