package org.apache.zookeeper.server.quorum;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.metrics.MetricsUtils;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerMetrics;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.test.ClientBase;
import org.junit.Test;
import org.junit.After;
import org.junit.Before;

public class EphemeralThrottleTest extends QuorumPeerTestBase {

  final static int MAX_EPHEMERAL_NODES = 32;
  final static int NUM_SERVERS = 5;
  final static String PATH = "/eph-test";

  @Before
  public void setUp() throws Exception {
    ClientBase.setupTestEnv();

    // just to get rid of the unrelated 'InstanceAlreadyExistsException' in the logs
    System.setProperty("zookeeper.jmx.log4j.disable", "true");
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
    System.clearProperty("zookeeper.jmx.log4j.disable");
  }

  /*
   * TODO : Add following UT scenarios
   * 1. Creating ephemeral nodes through any server in quorum
   * 2. Removing session should delete all counters
   * 3. Restarting zookeeper server should keep count up-todate
   * 4. Fix create and then delete before commit test case.
   */
  @Test(expected = KeeperException.SessionEphemeralCountExceedException.class)
  public void limitingEphemeralsTest() throws Exception {
    System.setProperty("zookeeper.ephemeral.count.limit", Integer.toString(MAX_EPHEMERAL_NODES));
    servers = LaunchServers(NUM_SERVERS);
    for (int i = 0; i < MAX_EPHEMERAL_NODES+1; i++) {
      String retPath = servers.zk[0].create(PATH + i, new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }
  }

  @Test
  public void disabledEphemeralsTest() throws Exception {
    System.setProperty("zookeeper.ephemeral.count.limit", Integer.toString(-1));
    servers = LaunchServers(NUM_SERVERS);
    for (int i = 0; i < MAX_EPHEMERAL_NODES+1; i++) {
      servers.zk[0].create(PATH + "-" + i, new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }
  }

  @Test(expected = KeeperException.SessionEphemeralCountExceedException.class)
  public void limitingSequentialEphemeralsTest() throws Exception {
    System.setProperty("zookeeper.ephemeral.count.limit", Integer.toString(MAX_EPHEMERAL_NODES));
    servers = LaunchServers(NUM_SERVERS);
    for (int i = 0; i < MAX_EPHEMERAL_NODES+1; i++) {
      servers.zk[0].create(PATH, new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    }
  }

  /**
   *  Verify that the ephemeral limit enforced correctly when there are delete operations.
   */
  @Test
  public void limitingEphemeralsWithDeletesTest() throws Exception {
    int numDelete = 8;
    System.setProperty("zookeeper.ephemeral.count.limit", Integer.toString(MAX_EPHEMERAL_NODES));
    servers = LaunchServers(NUM_SERVERS);
    for (int i = 0; i < MAX_EPHEMERAL_NODES/2; i++) {
      servers.zk[0].create(PATH + "-" + i, new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }
    for (int i = 0; i < numDelete; i++) {
      servers.zk[0].delete(PATH + "-" + i, -1);
    }
    for (int i = 0; i < (MAX_EPHEMERAL_NODES/2) + numDelete; i++) {
      servers.zk[0].create(PATH + "-" + (i+MAX_EPHEMERAL_NODES), new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    boolean threw = false;
    try {
      servers.zk[0].create(PATH + "-0", new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    } catch (KeeperException.SessionEphemeralCountExceedException e) {
      threw = true;
    }
    assertTrue(threw);
  }

  /**
   *  Check that our emitted metric around the number of request rejections from too many ephemerals is accurate.
   */
  @Test
  public void rejectedEphemeralCreatesMetricsTest() throws Exception {
    int ephemeralExcess = 8;
    System.setProperty("zookeeper.ephemeral.count.limit", Integer.toString(MAX_EPHEMERAL_NODES));
    servers = LaunchServers(NUM_SERVERS);
    for (int i = 0; i < MAX_EPHEMERAL_NODES + ephemeralExcess; i++) {
      try {
        servers.zk[0].create(PATH + "-" + i, new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
      } catch (KeeperException.SessionEphemeralCountExceedException e) {
        LOG.info("Encountered SessionEphemeralCountExceedException as expected, continuing...");
      }
    }

    long actual = (long) MetricsUtils.currentServerMetrics().get("ephemeral_violation_request_rejection_count");
    assertEquals(ephemeralExcess, actual);
  }

  /**
   *  Test that the ephemeral limit is accurate in the case where an ephemeral node is deleted before it is committed.
   */

  CountDownLatch latch = null;
  @Test
  public void createThenDeleteBeforeCommitTest() throws Exception {
    System.setProperty("zookeeper.ephemeral.count.limit", Integer.toString(MAX_EPHEMERAL_NODES));

    String hostPort = "127.0.0.1:" + PortAssignment.unique();
    File tmpDir = ClientBase.createTmpDir();
    ClientBase.setupTestEnv();
    ZooKeeperServer server = new ZooKeeperServerWithLatch(tmpDir, tmpDir, 3000);
    final int port = Integer.parseInt(hostPort.split(":")[1]);
    ServerCnxnFactory cnxnFactory = ServerCnxnFactory.createFactory(port, -1);
    ServerMetrics.getMetrics().resetAll();
    cnxnFactory.startup(server);

    latch = new CountDownLatch(1);

    ZooKeeper zk = ClientBase.createZKClient(hostPort);
    zk.create(PATH, new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    zk.delete(PATH, -1);

    latch.countDown();

    boolean noException = true;
    try {
      for (int i = 0; i < MAX_EPHEMERAL_NODES; i++) {
        zk.create(PATH + "-" + i, new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
      }
    } catch (KeeperException.SessionEphemeralCountExceedException e) {
      noException = false;
    }
    assertEquals(noException, true);

    boolean threw = false;
    try {
      zk.create(PATH + "-" + MAX_EPHEMERAL_NODES, new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    } catch (KeeperException.SessionEphemeralCountExceedException e) {
      threw = true;
    }
    assertEquals(threw, true);
  }

  class ZooKeeperServerWithLatch extends ZooKeeperServer {
    public ZooKeeperServerWithLatch(File snapDir, File logDir, int tickTime) throws IOException {
      super(snapDir, logDir, tickTime);
    }

    @Override
    public DataTree.ProcessTxnResult processTxn(Request request) {
      // TODO: Fix this latch since it doesn't really make sure that delete proceeds beacuse it's in same thread.
      if (latch != null) {
        try {
          latch.await(10, TimeUnit.SECONDS);
        } catch (Exception e) {}
      }
      DataTree.ProcessTxnResult res = super.processTxn(request);
      return res;
    }
  }
}