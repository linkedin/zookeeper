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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.test.ClientBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests that the "AcrossRestarts" backup staleness gauges survive a server restart.
 */
public class BackupStatsTest extends ZKTestCase {
  private static final long MINUTE = TimeUnit.MINUTES.toMillis(1);

  private final AtomicLong now = new AtomicLong(1_790_000_000_000L);
  private File statusDir;

  @Before
  public void setUp() throws IOException {
    statusDir = ClientBase.createTmpDir();
  }

  @Test
  public void testStalenessAcrossRestartsContinuesAfterRestart() {
    BackupStats beforeRestart = new BackupStats(statusDir, now::get);
    beforeRestart.setSnapshotBackupIterationStart();
    beforeRestart.setSnapshotBackupIterationDone(true);
    beforeRestart.setTxnLogBackupIterationStart();
    beforeRestart.setTxnLogBackupIterationDone(true);

    now.addAndGet(300 * MINUTE);
    BackupStats afterRestart = new BackupStats(statusDir, now::get);

    Assert.assertEquals(300, afterRestart.getMinutesSinceLastSuccessfulSnapshotIterationAcrossRestarts());
    Assert.assertEquals(300, afterRestart.getMinutesSinceLastSuccessfulTxnLogIterationAcrossRestarts());
    // The existing gauges still start over with the process.
    Assert.assertEquals(0, afterRestart.getMinutesSinceLastSuccessfulSnapshotIteration());
    Assert.assertEquals(0, afterRestart.getMinutesSinceLastSuccessfulTxnLogIteration());
  }

  @Test
  public void testFailedIterationKeepsStalenessAcrossRestarts() {
    BackupStats stats = new BackupStats(statusDir, now::get);
    stats.setTxnLogBackupIterationStart();
    stats.setTxnLogBackupIterationDone(true);
    now.addAndGet(40 * MINUTE);
    stats.setTxnLogBackupIterationStart();
    stats.setTxnLogBackupIterationDone(false);
    now.addAndGet(20 * MINUTE);

    Assert.assertEquals(60, stats.getMinutesSinceLastSuccessfulTxnLogIterationAcrossRestarts());
    Assert.assertEquals(60,
        new BackupStats(statusDir, now::get).getMinutesSinceLastSuccessfulTxnLogIterationAcrossRestarts());
  }

  @Test
  public void testSuccessfulIterationResetsStalenessAcrossRestarts() {
    BackupStats beforeRestart = new BackupStats(statusDir, now::get);
    beforeRestart.setTxnLogBackupIterationStart();
    beforeRestart.setTxnLogBackupIterationDone(true);
    now.addAndGet(300 * MINUTE);

    BackupStats afterRestart = new BackupStats(statusDir, now::get);
    afterRestart.setTxnLogBackupIterationStart();
    afterRestart.setTxnLogBackupIterationDone(true);
    now.addAndGet(10 * MINUTE);

    Assert.assertEquals(10, afterRestart.getMinutesSinceLastSuccessfulTxnLogIterationAcrossRestarts());
    Assert.assertEquals(10,
        new BackupStats(statusDir, now::get).getMinutesSinceLastSuccessfulTxnLogIterationAcrossRestarts());
  }

  @Test
  public void testBackupTypesAreTrackedSeparately() {
    BackupStats beforeRestart = new BackupStats(statusDir, now::get);
    beforeRestart.setTxnLogBackupIterationStart();
    beforeRestart.setTxnLogBackupIterationDone(true);
    now.addAndGet(100 * MINUTE);
    beforeRestart.setSnapshotBackupIterationStart();
    beforeRestart.setSnapshotBackupIterationDone(true);
    now.addAndGet(50 * MINUTE);

    BackupStats afterRestart = new BackupStats(statusDir, now::get);
    Assert.assertEquals(150, afterRestart.getMinutesSinceLastSuccessfulTxnLogIterationAcrossRestarts());
    Assert.assertEquals(50, afterRestart.getMinutesSinceLastSuccessfulSnapshotIterationAcrossRestarts());
  }

  @Test
  public void testStartsFromProcessStartWithoutUsablePersistedState() throws IOException {
    BackupStats fresh = new BackupStats(statusDir, now::get);
    now.addAndGet(10 * MINUTE);
    Assert.assertEquals(10, fresh.getMinutesSinceLastSuccessfulTxnLogIterationAcrossRestarts());

    fresh.setTxnLogBackupIterationStart();
    fresh.setTxnLogBackupIterationDone(true);
    File[] persisted = statusDir.listFiles();
    Assert.assertNotNull(persisted);
    Assert.assertTrue(persisted.length > 0);
    for (File file : persisted) {
      Files.write(file.toPath(), "not a timestamp".getBytes(StandardCharsets.UTF_8));
    }
    now.addAndGet(30 * MINUTE);
    Assert.assertEquals(0,
        new BackupStats(statusDir, now::get).getMinutesSinceLastSuccessfulTxnLogIterationAcrossRestarts());

    BackupStats withoutStatusDir = new BackupStats(null, now::get);
    withoutStatusDir.setTxnLogBackupIterationStart();
    withoutStatusDir.setTxnLogBackupIterationDone(true);
    Assert.assertEquals(0, withoutStatusDir.getMinutesSinceLastSuccessfulTxnLogIterationAcrossRestarts());
  }

  @Test
  public void testTimetableStalenessAcrossRestarts() {
    TimetableBackupStats beforeRestart = new TimetableBackupStats(statusDir, now::get);
    beforeRestart.setTimetableBackupIterationStart();
    beforeRestart.setTimetableBackupIterationDone(true);
    now.addAndGet(20 * MINUTE);
    beforeRestart.setTimetableBackupIterationStart();
    beforeRestart.setTimetableBackupIterationDone(false);
    now.addAndGet(230 * MINUTE);

    TimetableBackupStats afterRestart = new TimetableBackupStats(statusDir, now::get);
    Assert.assertEquals(250, afterRestart.getMinutesSinceLastSuccessfulTimetableIterationAcrossRestarts());
    Assert.assertEquals(0, afterRestart.getMinutesSinceLastSuccessfulTimetableIteration());
  }
}
