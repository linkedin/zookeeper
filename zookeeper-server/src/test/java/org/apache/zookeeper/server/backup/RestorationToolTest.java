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

package org.apache.zookeeper.server.backup;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.DummyWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.cli.RestoreCommand;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.SyncRequestProcessor;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.backup.storage.BackupStorageProvider;
import org.apache.zookeeper.server.backup.storage.BackupStorageUtil;
import org.apache.zookeeper.server.backup.storage.impl.FileSystemBackupStorage;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.persistence.Util;
import org.apache.zookeeper.server.persistence.ZxidRange;
import org.apache.zookeeper.test.ClientBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.mockito.Mockito.when;

public class RestorationToolTest extends ZKTestCase {
  private static final Logger LOG = LoggerFactory.getLogger(RestorationToolTest.class);
  private static final String HOSTPORT = "127.0.0.1:" + PortAssignment.unique();
  private static final int CONNECTION_TIMEOUT = 300000;
  private static final String TEST_NAMESPACE = "TEST_NAMESPACE";

  private ZooKeeper connection;
  private File dataDir;
  private File backupTmpDir;
  private File backupDir;
  private File backupStatusDir;
  private ZooKeeperServer zks;
  private ServerCnxnFactory serverCnxnFactory;
  private BackupStorageProvider backupStorage;
  private BackupConfig backupConfig;
  private Random random = new Random();
  private final int txnCnt = 1000;
  private File restoreDir;
  private FileTxnSnapLog restoreSnapLog;
  private File backupFileRootDir;
  private File restoreTempDir;
  private File timetableDir;
  private BackupManager backupManager;
  private long timestampInMiddle;

  @Before
  public void setup() throws Exception {
    backupStatusDir = ClientBase.createTmpDir();
    backupTmpDir = ClientBase.createTmpDir();
    dataDir = ClientBase.createTmpDir();
    backupDir = ClientBase.createTmpDir();
    restoreDir = ClientBase.createTmpDir();
    restoreTempDir = ClientBase.createTmpDir();
    timetableDir = ClientBase.createTmpDir();

    backupConfig = new BackupConfig.Builder().
        setEnabled(true).
        setStatusDir(backupStatusDir).
        setTmpDir(backupTmpDir).
        setBackupStoragePath(backupDir.getAbsolutePath()).
        setNamespace(TEST_NAMESPACE).
        setStorageProviderClassName(FileSystemBackupStorage.class.getName()).
        setTimetableEnabled(true).
        setTimetableBackupIntervalInMs(100L).
        setTimetableStoragePath(timetableDir.getPath()).
        build().get();
    backupStorage = new FileSystemBackupStorage(backupConfig);
    backupFileRootDir = new File(backupDir, TEST_NAMESPACE);

    ClientBase.setupTestEnv();

    LOG.info("Starting Zk");
    zks = new ZooKeeperServer(dataDir, dataDir, 3000);
    SyncRequestProcessor.setSnapCount(100);
    final int PORT = Integer.parseInt(HOSTPORT.split(":")[1]);
    serverCnxnFactory = ServerCnxnFactory.createFactory(PORT, -1);
    serverCnxnFactory.startup(zks);

    LOG.info("Waiting for server startup");
    Assert.assertTrue("waiting for server being up ",
        ClientBase.waitForServerUp(HOSTPORT, CONNECTION_TIMEOUT));

    connection = new ZooKeeper(HOSTPORT, CONNECTION_TIMEOUT, DummyWatcher.INSTANCE);
    restoreSnapLog = new FileTxnSnapLog(restoreDir, restoreDir);

    backupManager = new BackupManager(dataDir, dataDir, -1, backupConfig);
    backupManager.initialize();

    for (int i = 1; i < txnCnt; i++) {
      connection
          .create("/node" + i, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      if (getRandomBoolean(0.4f)) {
        zks.getTxnLogFactory().rollLog();
        backupManager.getLogBackup().run(1);
      }
      if (getRandomBoolean(0.2f)) {
        zks.takeSnapshot();
      }
      // Record a timestamp that's in the valid backup timestamp range, used to test restoration to timestamp
      if (i == txnCnt / 2) {
        timestampInMiddle = System.currentTimeMillis();
      }
    }

    backupManager.getLogBackup().run(1);
    backupManager.getSnapBackup().run(1);
  }

  @After
  public void teardown() throws Exception {
    if (connection != null) {
      connection.close();
    }
    connection = null;

    LOG.info("Closing and cleaning up Zk");

    LOG.info("Closing Zk");

    if (serverCnxnFactory != null) {
      serverCnxnFactory.closeAll(ServerCnxn.DisconnectReason.SERVER_SHUTDOWN);
      serverCnxnFactory.shutdown();
      serverCnxnFactory = null;
    }

    if (zks != null) {
      zks.getZKDatabase().close();
      zks.shutdown();
      zks = null;
    }

    Assert.assertTrue("waiting for server to shutdown",
        ClientBase.waitForServerDown(HOSTPORT, CONNECTION_TIMEOUT));

    serverCnxnFactory = null;
    zks = null;
    backupStorage = null;
  }

  @Test
  public void testSuccessfulRestorationToZxid() throws IOException, InterruptedException {
    for (int i = 0; i < 5; i++) {
      int restoreZxid = random.nextInt(txnCnt);
      File restoreTempDir = ClientBase.createTmpDir();
      RestoreFromBackupTool restoreTool =
          new RestoreFromBackupTool(backupStorage, restoreSnapLog, restoreZxid, false,
              restoreTempDir);
      restoreTool.run();
      validateRestoreCoverage(restoreZxid);

      // Clean up restoreDir for next test
      List<Path> filePaths =
          Files.walk(Paths.get(restoreDir.getPath())).filter(Files::isRegularFile)
              .collect(Collectors.toList());
      filePaths.forEach(filePath -> {
        try {
          Files.delete(filePath);
        } catch (IOException e) {
          e.printStackTrace();
        }
      });
    }
  }

  @Test
  public void testSuccessfulRestorationToLatest() throws IOException, InterruptedException {
    RestoreFromBackupTool restoreTool =
        new RestoreFromBackupTool(backupStorage, restoreSnapLog, Long.MAX_VALUE, false,
            restoreTempDir);
    restoreTool.run();
    validateRestoreCoverage(txnCnt);
  }

  @Test
  public void testFailedRestorationWithOutOfRangeZxid() throws IOException {
    try {
      RestoreFromBackupTool restoreTool =
          new RestoreFromBackupTool(backupStorage, restoreSnapLog, txnCnt + 1, false,
              restoreTempDir);
      restoreTool.run();
      Assert.fail(
          "The restoration should fail because the zxid restoration point specified is out of range.");
    } catch (IllegalArgumentException e) {
      e.printStackTrace();
    } catch (Exception e1) {
      Assert.fail("RestoreException should be thrown.");
    }
  }

  @Test
  public void testFailedRestorationWithLostLog() {
    FilenameFilter filter = (dir, name) -> name.startsWith(Util.TXLOG_PREFIX);
    File[] backupLogs = backupFileRootDir.listFiles(filter);
    Assert.assertNotNull(backupLogs);
    for (File backupLog : backupLogs) {
      backupLog.renameTo(
          new File(backupLog.getPath().replaceAll(Util.TXLOG_PREFIX, BackupUtil.LOST_LOG_PREFIX)));
    }
    try {
      RestoreFromBackupTool restoreTool =
          new RestoreFromBackupTool(backupStorage, restoreSnapLog, txnCnt, false, restoreTempDir);
      restoreTool.run();
      Assert.fail("The restoration should fail because the transaction logs are lost logs.");
    } catch (IllegalArgumentException e) {
      e.printStackTrace();
    } catch (Exception e1) {
      Assert.fail("RestoreException should be thrown.");
    }
  }

  private void validateRestoreCoverage(int restoreZxid) throws IOException {
    // Test restored snapshots
    File restoredSnapshot = restoreSnapLog.findMostRecentSnapshot();
    Assert.assertNotNull(restoredSnapshot);
    // File name is changed when file is restored to local, so go to backup storage to find the whole zxid range
    File[] matchedSnapshotInBackupStorage =
        BackupStorageUtil.getFilesWithPrefix(backupFileRootDir, restoredSnapshot.getName() + "-");
    Assert.assertEquals(1, matchedSnapshotInBackupStorage.length);
    ZxidRange snapZxidRange =
        Util.getZxidRangeFromName(matchedSnapshotInBackupStorage[0].getName(), Util.SNAP_PREFIX);
    System.out.println(Arrays.toString(matchedSnapshotInBackupStorage));
    Assert.assertTrue("High zxid of restored snapshot " + Long.toHexString(snapZxidRange.getHigh())
            + " is larger than the restoreZxid " + Long.toHexString(restoreZxid) + ".",
        snapZxidRange.getHigh() <= restoreZxid);

    // Test restored txn logs
    List<File> restoredLogs =
        Arrays.asList(Objects.requireNonNull(restoreSnapLog.getDataDir().listFiles()));
    restoredLogs.sort(new Comparator<File>() {
      @Override
      public int compare(File o1, File o2) {
        long o1Zxid = Util.getZxidFromName(o1.getName(), Util.TXLOG_PREFIX);
        long o2Zxid = Util.getZxidFromName(o2.getName(), Util.TXLOG_PREFIX);
        return Long.compare(o1Zxid, o2Zxid);
      }
    });
    Assert.assertTrue(
        "The oldest restored txn log has a higher zxid than the low zxid of restored snapshot.",
        Util.getZxidFromName(restoredLogs.get(0).getName(), Util.TXLOG_PREFIX) <= snapZxidRange
            .getLow());
    Assert.assertTrue("The restored files does not cover to the restoreZxid.",
        restoreSnapLog.getLastLoggedZxid() >= restoreZxid);

    // Validate all the zxids are covered
    boolean[] coveredZxid = new boolean[txnCnt + 1];
    for (File restoredLog : restoredLogs) {
      if (!restoredLog.getName().startsWith(Util.TXLOG_PREFIX)) {
        continue;
      }
      File[] matchedLogInBackupStorage =
          BackupStorageUtil.getFilesWithPrefix(backupFileRootDir, restoredLog.getName() + "-");
      Assert.assertEquals(
          "Number of matched transaction log file for file " + restoredLog.getName() + " is not 1.",
          1, matchedLogInBackupStorage.length);
      ZxidRange logZxidRange =
          Util.getZxidRangeFromName(matchedLogInBackupStorage[0].getName(), Util.TXLOG_PREFIX);
      int lowZxid = (int) logZxidRange.getLow();
      int highZxid = (int) logZxidRange.getHigh();
      for (int i = lowZxid; i <= highZxid; i++) {
        coveredZxid[i] = true;
      }
    }

    for (int i = (int) snapZxidRange.getLow(); i <= restoreZxid; i++) {
      Assert.assertTrue("Zxid " + Long.toHexString(i) + " is not covered in the restoration.",
          coveredZxid[i]);
    }
  }

  private boolean getRandomBoolean(float p) {
    return random.nextFloat() < p;
  }

  @Test
  public void testRestoreToZxidByCommandLine() throws IOException {
    //Restore to a random zxid
    int restoreZxid = random.nextInt(txnCnt);
    RestoreFromBackupTool restoreTool = new RestoreFromBackupTool();
    CommandLine cl = Mockito.mock(CommandLine.class);
    when(cl.hasOption(RestoreCommand.OptionShortForm.RESTORE_ZXID)).thenReturn(true);
    when(cl.hasOption(RestoreCommand.OptionShortForm.BACKUP_STORE)).thenReturn(true);
    when(cl.hasOption(RestoreCommand.OptionShortForm.SNAP_DESTINATION)).thenReturn(true);
    when(cl.hasOption(RestoreCommand.OptionShortForm.LOG_DESTINATION)).thenReturn(true);
    when(cl.getOptionValue(RestoreCommand.OptionShortForm.RESTORE_ZXID))
        .thenReturn(String.valueOf(restoreZxid));
    when(cl.getOptionValue(RestoreCommand.OptionShortForm.BACKUP_STORE))
        .thenReturn("gpfs::" + backupDir.getPath() + ":" + TEST_NAMESPACE);
    when(cl.getOptionValue(RestoreCommand.OptionShortForm.SNAP_DESTINATION))
        .thenReturn(restoreDir.getPath());
    when(cl.getOptionValue(RestoreCommand.OptionShortForm.LOG_DESTINATION))
        .thenReturn(restoreDir.getPath());
    Assert.assertTrue(restoreTool.runWithRetries(cl));
    validateRestoreCoverage(restoreZxid);

    //Restore to latest
    //Use an empty directory as restoration destination; otherwise, the restoration will fail
    restoreDir = ClientBase.createTmpDir();
    restoreSnapLog = new FileTxnSnapLog(restoreDir, restoreDir);
    when(cl.getOptionValue(RestoreCommand.OptionShortForm.SNAP_DESTINATION))
        .thenReturn(restoreDir.getPath());
    when(cl.getOptionValue(RestoreCommand.OptionShortForm.LOG_DESTINATION))
        .thenReturn(restoreDir.getPath());
    when(cl.getOptionValue(RestoreCommand.OptionShortForm.RESTORE_ZXID))
        .thenReturn(BackupUtil.LATEST);
    Assert.assertTrue(restoreTool.runWithRetries(cl));
    validateRestoreCoverage(txnCnt);
  }

  @Test
  public void testRestoreToTimestampByCommandLine() throws IOException {
    //Test restoration CLI using a timestamp recorded in the midpoint of the test ZNode creation
    backupManager.getTimetableBackup().run(1);
    RestoreFromBackupTool restoreTool = new RestoreFromBackupTool();
    CommandLine cl = Mockito.mock(CommandLine.class);
    when(cl.hasOption(RestoreCommand.OptionShortForm.RESTORE_ZXID)).thenReturn(false);
    when(cl.hasOption(RestoreCommand.OptionShortForm.RESTORE_TIMESTAMP)).thenReturn(true);
    when(cl.hasOption(RestoreCommand.OptionShortForm.BACKUP_STORE)).thenReturn(true);
    when(cl.hasOption(RestoreCommand.OptionShortForm.SNAP_DESTINATION)).thenReturn(true);
    when(cl.hasOption(RestoreCommand.OptionShortForm.LOG_DESTINATION)).thenReturn(true);
    when(cl.hasOption(RestoreCommand.OptionShortForm.TIMETABLE_STORAGE_PATH)).thenReturn(true);
    when(cl.getOptionValue(RestoreCommand.OptionShortForm.RESTORE_TIMESTAMP))
        .thenReturn(String.valueOf(timestampInMiddle));
    when(cl.getOptionValue(RestoreCommand.OptionShortForm.BACKUP_STORE))
        .thenReturn("gpfs::" + backupDir.getPath() + ":" + TEST_NAMESPACE);
    when(cl.getOptionValue(RestoreCommand.OptionShortForm.SNAP_DESTINATION))
        .thenReturn(restoreDir.getPath());
    when(cl.getOptionValue(RestoreCommand.OptionShortForm.LOG_DESTINATION))
        .thenReturn(restoreDir.getPath());
    when(cl.getOptionValue(RestoreCommand.OptionShortForm.TIMETABLE_STORAGE_PATH))
        .thenReturn(timetableDir.getPath() + "/" + TEST_NAMESPACE);
    Assert.assertTrue(restoreTool.runWithRetries(cl));

    //Restore to latest using timestamp
    //Use an empty directory as restoration destination; otherwise, the restoration will fail
    restoreDir = ClientBase.createTmpDir();
    restoreSnapLog = new FileTxnSnapLog(restoreDir, restoreDir);
    when(cl.getOptionValue(RestoreCommand.OptionShortForm.SNAP_DESTINATION))
        .thenReturn(restoreDir.getPath());
    when(cl.getOptionValue(RestoreCommand.OptionShortForm.LOG_DESTINATION))
        .thenReturn(restoreDir.getPath());
    when(cl.getOptionValue(RestoreCommand.OptionShortForm.RESTORE_TIMESTAMP))
        .thenReturn(BackupUtil.LATEST);
    Assert.assertTrue(restoreTool.runWithRetries(cl));
    validateRestoreCoverage(txnCnt);
  }

  @Test
  public void testSpotRestorationTool() throws IOException, InterruptedException, KeeperException {
    //TEST 1.
    //Target node: /testsr
    // This test is going to test three scenarios:
    //    1. Nodes exist in backup files but not in the running zk server
    //        Path => value:
    //          /testsr/restore => restore
    //          /testsr/restore/node0 => restore0
    //        Expected: nodes are created in the running zk server
    //    2. Nodes not exist in backup files but in the running zk server
    //        Path => value:
    //          /testsr/new => new
    //          /testsr/new/node0 => new0
    //        Expected: messages printed at the end indicate the node is skipped
    //    3. Nodes exist in both backup files and in the running zk server, but with different values
    //        Path => value in backup files; value in running server
    //          /testsr/existing => restoredVal; existingVal
    //        Expected: messages printed at the end indicate the node is skipped

    // Create several znodes in original zk server whose data will be backed up for testing
    connection.create("/testsr", "restoredTarget".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);
    connection.create("/testsr/restore", "restore".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);
    connection.create("/testsr/restore/node0", "restore0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);
    connection.create("/testsr/existing", "restoredVal".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);
    backupManager.getLogBackup().run(1);
    backupManager.getSnapBackup().run(1);

    // Close the original zk server and zk client;
    //start a new server as the server to be restored, and a new client connection
    connection.close();
    zks.shutdown();
    LOG.info("ZK server is shut down.");

    dataDir = ClientBase.createTmpDir();
    LOG.info("Starting a new zk server.");
    zks = new ZooKeeperServer(dataDir, dataDir, 3000);
    SyncRequestProcessor.setSnapCount(100);
    serverCnxnFactory.startup(zks);

    LOG.info("Waiting for server startup");
    Assert.assertTrue("waiting for server being up",
        ClientBase.waitForServerUp(HOSTPORT, CONNECTION_TIMEOUT));
    connection = new ZooKeeper(HOSTPORT, CONNECTION_TIMEOUT, DummyWatcher.INSTANCE);

    // Create several znodes in the running zk server
    connection
        .create("/testsr", "target".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    connection.create("/testsr/new", "new".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);
    connection.create("/testsr/new/node0", "new0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);
    connection.create("/testsr/existing", "existingVal".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);

    // Copy backup files to restoreDir
    RestoreFromBackupTool restoreTool = new RestoreFromBackupTool();
    CommandLine cl = Mockito.mock(CommandLine.class);
    when(cl.hasOption(RestoreCommand.OptionShortForm.RESTORE_ZXID)).thenReturn(true);
    when(cl.hasOption(RestoreCommand.OptionShortForm.BACKUP_STORE)).thenReturn(true);
    when(cl.hasOption(RestoreCommand.OptionShortForm.SNAP_DESTINATION)).thenReturn(true);
    when(cl.hasOption(RestoreCommand.OptionShortForm.LOG_DESTINATION)).thenReturn(true);
    when(cl.getOptionValue(RestoreCommand.OptionShortForm.RESTORE_ZXID))
        .thenReturn((BackupUtil.LATEST));
    when(cl.getOptionValue(RestoreCommand.OptionShortForm.BACKUP_STORE))
        .thenReturn("gpfs::" + backupDir.getPath() + ":" + TEST_NAMESPACE);
    when(cl.getOptionValue(RestoreCommand.OptionShortForm.SNAP_DESTINATION))
        .thenReturn(restoreDir.getPath());
    when(cl.getOptionValue(RestoreCommand.OptionShortForm.LOG_DESTINATION))
        .thenReturn(restoreDir.getPath());
    Assert.assertTrue(restoreTool.runWithRetries(cl));

    // Do spot restoration using the backup files
    String targetZnodePath = "/testsr";
    MockSpotRestorationTool tool =
        new MockSpotRestorationTool(restoreDir, connection, targetZnodePath, true);
    tool.run();

    connection = new ZooKeeper(HOSTPORT, CONNECTION_TIMEOUT, DummyWatcher.INSTANCE);

    // The target node's value should be updated
    Assert.assertArrayEquals("restoredTarget".getBytes(),
        connection.getData("/testsr", false, new Stat()));

    // These nodes should be created in the zk server
    Assert.assertArrayEquals("restore".getBytes(),
        connection.getData("/testsr/restore", false, new Stat()));
    Assert.assertArrayEquals("restore0".getBytes(),
        connection.getData("/testsr/restore/node0", false, new Stat()));

    // We do not delete new nodes, only log them in the messages
    Assert.assertNotNull(connection.exists("/testsr/new", false));
    Assert.assertNotNull(connection.exists("/testsr/new/node0", false));

    // We do not update existing nodes' values, only log them in the messages
    Assert.assertArrayEquals("existingVal".getBytes(),
        connection.getData("/testsr/existing", false, new Stat()));

    LOG.info("Please examine the messages printed out. "
        + "There should be messages for skipping nodes: \"/testsr/new\" and \"/testsr/existing\".");

    //TEST 2.
    //  Test target node's parent nodes do not exist
    connection.close();
    zks.shutdown();
    LOG.info("ZK server is shut down.");

    dataDir = ClientBase.createTmpDir();
    LOG.info("Starting a new zk server.");
    zks = new ZooKeeperServer(dataDir, dataDir, 3000);
    SyncRequestProcessor.setSnapCount(100);
    serverCnxnFactory.startup(zks);

    LOG.info("Waiting for server startup");
    Assert.assertTrue("waiting for server being up",
        ClientBase.waitForServerUp(HOSTPORT, CONNECTION_TIMEOUT));
    connection = new ZooKeeper(HOSTPORT, CONNECTION_TIMEOUT, DummyWatcher.INSTANCE);

    targetZnodePath = "/testsr/restore/node0";
    tool = new MockSpotRestorationTool(restoreDir, connection, targetZnodePath, true);
    tool.run();

    connection = new ZooKeeper(HOSTPORT, CONNECTION_TIMEOUT, DummyWatcher.INSTANCE);

    Assert.assertArrayEquals("restoredTarget".getBytes(),
        connection.getData("/testsr", false, new Stat()));
    Assert.assertArrayEquals("restore".getBytes(),
        connection.getData("/testsr/restore", false, new Stat()));
    Assert.assertArrayEquals("restore0".getBytes(),
        connection.getData("/testsr/restore/node0", false, new Stat()));
  }

  @Test
  public void testSpotRestorationByCommandLine() throws IOException, InterruptedException {
    //Test restoration CLI using a timestamp recorded in the midpoint of the test ZNode creation
    backupManager.getTimetableBackup().run(1);

    // Close the original zk server and zk client;
    //start a new server as the server to be restored
    connection.close();
    zks.shutdown();
    LOG.info("ZK server is shut down.");

    dataDir = ClientBase.createTmpDir();
    LOG.info("Starting a new zk server.");
    zks = new ZooKeeperServer(dataDir, dataDir, 3000);
    SyncRequestProcessor.setSnapCount(100);
    serverCnxnFactory.startup(zks);

    LOG.info("Waiting for server startup");
    Assert.assertTrue("waiting for server being up",
        ClientBase.waitForServerUp(HOSTPORT, CONNECTION_TIMEOUT));

    // Mock CLI command
    CommandLine cl = Mockito.mock(CommandLine.class);
    when(cl.hasOption(RestoreCommand.OptionShortForm.RESTORE_ZXID)).thenReturn(false);
    when(cl.hasOption(RestoreCommand.OptionShortForm.RESTORE_TIMESTAMP)).thenReturn(true);
    when(cl.hasOption(RestoreCommand.OptionShortForm.BACKUP_STORE)).thenReturn(true);
    when(cl.hasOption(RestoreCommand.OptionShortForm.SNAP_DESTINATION)).thenReturn(true);
    when(cl.hasOption(RestoreCommand.OptionShortForm.LOG_DESTINATION)).thenReturn(true);
    when(cl.hasOption(RestoreCommand.OptionShortForm.TIMETABLE_STORAGE_PATH)).thenReturn(true);
    when(cl.hasOption(RestoreCommand.OptionShortForm.ZNODE_PATH_TO_RESTORE)).thenReturn(true);
    when(cl.hasOption(RestoreCommand.OptionShortForm.ZK_SERVER_CONNECTION_STRING)).thenReturn(true);
    when(cl.getOptionValue(RestoreCommand.OptionShortForm.RESTORE_TIMESTAMP))
        .thenReturn(String.valueOf(timestampInMiddle));
    when(cl.getOptionValue(RestoreCommand.OptionShortForm.BACKUP_STORE))
        .thenReturn("gpfs::" + backupDir.getPath() + ":" + TEST_NAMESPACE);
    when(cl.getOptionValue(RestoreCommand.OptionShortForm.SNAP_DESTINATION))
        .thenReturn(restoreDir.getPath());
    when(cl.getOptionValue(RestoreCommand.OptionShortForm.LOG_DESTINATION))
        .thenReturn(restoreDir.getPath());
    when(cl.getOptionValue(RestoreCommand.OptionShortForm.TIMETABLE_STORAGE_PATH))
        .thenReturn(timetableDir.getPath() + "/" + TEST_NAMESPACE);
    // Restore the first node created, so we are sure this node exists at the moment of timestamp provided
    String nodePath = "/node1";
    when(cl.getOptionValue(RestoreCommand.OptionShortForm.ZNODE_PATH_TO_RESTORE))
        .thenReturn(nodePath);
    when(cl.getOptionValue(RestoreCommand.OptionShortForm.ZK_SERVER_CONNECTION_STRING))
        .thenReturn(HOSTPORT);

    // Run restoration
    RestoreFromBackupTool restoreTool = new MockRestoreFromBackupTool();
    Assert.assertTrue(restoreTool.runWithRetries(cl));
    Assert.assertNotNull(zks.getZKDatabase().getNode(nodePath));
  }

  class MockSpotRestorationTool extends SpotRestorationTool {
    public MockSpotRestorationTool(File dataDir, ZooKeeper zk, String targetZNodePath,
        boolean restoreRecursively) throws IOException {
      super(dataDir, zk, targetZNodePath, restoreRecursively);
    }

    @Override
    protected boolean getUserConfirmation(String requestMsg, String yesMsg, String noMsg) {
      return true;
    }
  }

  class MockRestoreFromBackupTool extends RestoreFromBackupTool {
    @Override
    protected void performSpotRestoration(File restoreTempDir)
        throws IOException, InterruptedException {
      LOG.info("Starting spot restoration for zk path " + znodePathToRestore);
      zk = new ZooKeeper(zkServerConnectionStr, CONNECTION_TIMEOUT, (event) -> {
        LOG.info("WATCHER:: client-server connection event received for spot restoration: " + event
            .toString());
      });
      spotRestorationTool =
          new MockSpotRestorationTool(restoreTempDir, zk, znodePathToRestore, restoreRecursively);
      spotRestorationTool.run();
    }
  }
}
