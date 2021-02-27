package org.apache.zookeeper.server.backup;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.DummyWatcher;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.SyncRequestProcessor;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.backup.exception.RestoreException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestorationToolTest extends ZKTestCase {
  private static final Logger LOG = LoggerFactory.getLogger(RestorationToolTest.class);
  private static final String HOSTPORT = "127.0.0.1:" + PortAssignment.unique();
  private static final int CONNECTION_TIMEOUT = 300000;
  private static final String TEST_NAMESPACE = "TEST_NAMESPACE";

  private ZooKeeper connection;
  private File dataDir;
  private File backupTmpDir;
  private File backupDir;
  private ZooKeeperServer zks;
  private ServerCnxnFactory serverCnxnFactory;
  private BackupStorageProvider backupStorage;
  private BackupConfig backupConfig;
  private Random random = new Random();
  private final int txnCnt = 1000;
  private File restoreDir;
  private FileTxnSnapLog restoreSnapLog;
  private File backupFileRootDir;

  @Before
  public void setup() throws Exception {
    dataDir = ClientBase.createTmpDir();
    backupTmpDir = ClientBase.createTmpDir();
    backupDir = ClientBase.createTmpDir();
    restoreDir = ClientBase.createTmpDir();

    backupConfig = new BackupConfig.Builder().
        setEnabled(true).
        setStatusDir(testBaseDir).
        setTmpDir(testBaseDir).
        setBackupStoragePath(backupDir.getAbsolutePath()).
        setNamespace(TEST_NAMESPACE).
        setStorageProviderClassName(FileSystemBackupStorage.class.getName()).
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

    BackupManager backupManager = new BackupManager(dataDir, dataDir, dataDir, backupTmpDir, 15,
        new FileSystemBackupStorage(backupConfig), TEST_NAMESPACE, -1);
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
    }
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
  public void testSuccessfulRestorationToZxid() throws IOException {
    for (int i = 0; i < 5; i++) {
      int restoreZxid = random.nextInt(txnCnt);
      RestoreFromBackupTool restoreTool =
          new RestoreFromBackupTool(backupStorage, restoreSnapLog, restoreZxid, false);
      restoreTool.run();
      validateRestoreCoverage(restoreZxid);
      FileUtils.deleteDirectory(restoreDir);
    }
  }

  @Test
  public void testSuccessfulRestorationToLatest() throws IOException {
    RestoreFromBackupTool restoreTool =
        new RestoreFromBackupTool(backupStorage, restoreSnapLog, Long.MAX_VALUE, false);
    restoreTool.run();
    validateRestoreCoverage(txnCnt);
    FileUtils.deleteDirectory(restoreDir);
  }

  @Test
  public void testFailedRestorationWithOutOfRangeZxid() throws IOException {
    try {
      RestoreFromBackupTool restoreTool =
          new RestoreFromBackupTool(backupStorage, restoreSnapLog, txnCnt + 1, false);
      restoreTool.run();
      Assert.fail(
          "The restoration should fail because the zxid restoration point specified is out of range.");
    } catch (RestoreException e) {
      // Do nothing
    } catch (Exception e1) {
      Assert.fail("RestoreException should be thrown.");
    }
  }

  @Test
  public void testFailedRestorationWithLostLog() {

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
    Assert.assertTrue(snapZxidRange.getHigh() <= restoreZxid);

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
        Util.getZxidFromName(restoredLogs.get(0).getName(), Util.TXLOG_PREFIX) <= snapZxidRange
            .getLow());
    Assert.assertTrue(restoreSnapLog.getLastLoggedZxid() >= restoreZxid);

    // Validate all the zxids are covered
    boolean[] coveredZxid = new boolean[txnCnt + 1];
    for (File restoredLog : restoredLogs) {
      if (restoredLog.getName().startsWith(Util.SNAP_PREFIX)) {
        continue;
      }
      File[] matchedLogInBackupStorage =
          BackupStorageUtil.getFilesWithPrefix(backupFileRootDir, restoredLog.getName() + "-");
      Assert.assertEquals(1, matchedLogInBackupStorage.length);
      ZxidRange logZxidRange =
          Util.getZxidRangeFromName(matchedLogInBackupStorage[0].getName(), Util.TXLOG_PREFIX);
      int lowZxid = (int) logZxidRange.getLow();
      int highZxid = (int) logZxidRange.getHigh();
      for (int i = lowZxid; i <= highZxid; i++) {
        coveredZxid[i] = true;
      }
    }

    for (int i = (int) snapZxidRange.getLow(); i <= restoreZxid; i++) {
      Assert.assertTrue(coveredZxid[i]);
    }
  }

  private boolean getRandomBoolean(float p) {
    return random.nextFloat() < p;
  }
}
