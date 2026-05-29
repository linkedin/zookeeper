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

package org.apache.zookeeper.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.persistence.SnapStream;
import org.apache.zookeeper.server.persistence.Util;
import org.apache.zookeeper.test.ClientBase;
import org.junit.Test;

public class ZooKeeperServerTest extends ZKTestCase {

    @Test
    public void testSortDataDirAscending() {
        File[] files = new File[5];

        files[0] = new File("foo.10027c6de");
        files[1] = new File("foo.10027c6df");
        files[2] = new File("bar.10027c6dd");
        files[3] = new File("foo.10027c6dc");
        files[4] = new File("foo.20027c6dc");

        File[] orig = files.clone();

        List<File> filelist = Util.sortDataDir(files, "foo", true);

        assertEquals(orig[2], filelist.get(0));
        assertEquals(orig[3], filelist.get(1));
        assertEquals(orig[0], filelist.get(2));
        assertEquals(orig[1], filelist.get(3));
        assertEquals(orig[4], filelist.get(4));
    }

    @Test
    public void testSortDataDirDescending() {
        File[] files = new File[5];

        files[0] = new File("foo.10027c6de");
        files[1] = new File("foo.10027c6df");
        files[2] = new File("bar.10027c6dd");
        files[3] = new File("foo.10027c6dc");
        files[4] = new File("foo.20027c6dc");

        File[] orig = files.clone();

        List<File> filelist = Util.sortDataDir(files, "foo", false);

        assertEquals(orig[4], filelist.get(0));
        assertEquals(orig[1], filelist.get(1));
        assertEquals(orig[0], filelist.get(2));
        assertEquals(orig[3], filelist.get(3));
        assertEquals(orig[2], filelist.get(4));
    }

    @Test
    public void testGetLogFiles() {
        File[] files = new File[5];

        files[0] = new File("log.10027c6de");
        files[1] = new File("log.10027c6df");
        files[2] = new File("snapshot.10027c6dd");
        files[3] = new File("log.10027c6dc");
        files[4] = new File("log.20027c6dc");

        File[] orig = files.clone();

        File[] filelist = FileTxnLog.getLogFiles(files, Long.parseLong("10027c6de", 16));

        assertEquals(3, filelist.length);
        assertEquals(orig[0], filelist[0]);
        assertEquals(orig[1], filelist[1]);
        assertEquals(orig[4], filelist[2]);
    }

    @Test
    public void testForceSyncDefaultEnabled() {
        File file = new File("foo.10027c6de");
        FileTxnLog log = new FileTxnLog(file);
        assertTrue(log.isForceSync());
    }

    @Test
    public void testForceSyncDefaultDisabled() {
        try {
            File file = new File("foo.10027c6de");
            System.setProperty("zookeeper.forceSync", "no");
            FileTxnLog log = new FileTxnLog(file);
            assertFalse(log.isForceSync());
        } finally {
            //Reset back to default.
            System.setProperty("zookeeper.forceSync", "yes");
        }
    }

    @Test
    public void testInvalidSnapshot() {
        File f = null;
        File tmpFileDir = null;
        try {
            tmpFileDir = ClientBase.createTmpDir();
            f = new File(tmpFileDir, "snapshot.0");
            if (!f.exists()) {
                f.createNewFile();
            }
            assertFalse("Snapshot file size is greater than 9 bytes", SnapStream.isValidSnapshot(f));
            assertTrue("Can't delete file", f.delete());
        } catch (IOException e) {
        } finally {
            if (null != tmpFileDir) {
                ClientBase.recursiveDelete(tmpFileDir);
            }
        }
    }

    /**
     * Helper: create a ZooKeeperServer with an initialized database.
     * This simulates the state during leader election where the DB
     * is already loaded (QuorumPeer.start() loads it before election).
     */
    private ZooKeeperServer createServerWithInitializedDb(File tmpDir) throws Exception {
        FileTxnSnapLog snapLog = new FileTxnSnapLog(tmpDir, tmpDir);
        ZKDatabase zkDb = new ZKDatabase(snapLog);
        // Initialize DB (as QuorumPeer.start() would do before election)
        zkDb.loadDataBase();
        ZooKeeperServer zks = new ZooKeeperServer(snapLog, 2000, null);
        zks.setZKDatabase(zkDb);
        return zks;
    }

    private static int countSnapshotFiles(File dir) {
        if (dir == null || !dir.exists()) {
            return 0;
        }
        File[] snaps = dir.listFiles((d, name) -> name.startsWith("snapshot."));
        return snaps == null ? 0 : snaps.length;
    }

    /**
     * Test that loadData(false) takes a snapshot (default behavior).
     * We detect this by checking that the snapshot file's modification
     * time is updated (the file name stays the same since zxid is unchanged).
     */
    @Test
    public void testLoadDataTakesSnapshotByDefault() throws Exception {
        File tmpDir = ClientBase.createTmpDir();
        try {
            ZooKeeperServer zks = createServerWithInitializedDb(tmpDir);
            File snapDir = new File(tmpDir, "version-2");
            long lastModBefore = getLatestSnapshotModTime(snapDir);
            // 1100ms covers HFS+ 1s mtime granularity on macOS dev hosts (APFS/ext4 have finer)
            Thread.sleep(1100);

            zks.loadData(false);

            long lastModAfter = getLatestSnapshotModTime(snapDir);
            assertTrue("Snapshot file should be updated when skipSnapshot=false",
                    lastModAfter > lastModBefore);
        } finally {
            ClientBase.recursiveDelete(tmpDir);
        }
    }

    /**
     * Test that loadData(true) skips the snapshot — file is NOT rewritten.
     */
    @Test
    public void testLoadDataSkipsSnapshotWhenRequested() throws Exception {
        File tmpDir = ClientBase.createTmpDir();
        try {
            ZooKeeperServer zks = createServerWithInitializedDb(tmpDir);
            File snapDir = new File(tmpDir, "version-2");
            long lastModBefore = getLatestSnapshotModTime(snapDir);
            Thread.sleep(1100);

            zks.loadData(true);

            long lastModAfter = getLatestSnapshotModTime(snapDir);
            assertEquals("Snapshot file should NOT be updated when skipSnapshot=true",
                    lastModBefore, lastModAfter);
        } finally {
            ClientBase.recursiveDelete(tmpDir);
        }
    }

    /**
     * Test that loadData() no-arg delegates to loadData(false) — takes snapshot.
     */
    @Test
    public void testLoadDataNoArgDelegatesToDefault() throws Exception {
        File tmpDir = ClientBase.createTmpDir();
        try {
            ZooKeeperServer zks = createServerWithInitializedDb(tmpDir);
            File snapDir = new File(tmpDir, "version-2");
            long lastModBefore = getLatestSnapshotModTime(snapDir);
            Thread.sleep(1100);

            zks.loadData();

            long lastModAfter = getLatestSnapshotModTime(snapDir);
            assertTrue("No-arg loadData() should take snapshot (backward compatible)",
                    lastModAfter > lastModBefore);
        } finally {
            ClientBase.recursiveDelete(tmpDir);
        }
    }

    private static long getLatestSnapshotModTime(File dir) {
        if (dir == null || !dir.exists()) {
            return 0;
        }
        File[] snaps = dir.listFiles((d, name) -> name.startsWith("snapshot."));
        if (snaps == null || snaps.length == 0) {
            return 0;
        }
        long latest = 0;
        for (File f : snaps) {
            latest = Math.max(latest, f.lastModified());
        }
        return latest;
    }

}
