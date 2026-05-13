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

package org.apache.zookeeper.server.auth.znode.groupacl;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.DummyWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.common.SpiffeAuthTestUtil;
import org.apache.zookeeper.server.MockServerCnxn;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.auth.ServerAuthenticationProvider;
import org.apache.zookeeper.server.watch.WatchesReport;
import org.apache.zookeeper.test.ClientBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ZkClientUriDomainMappingHelperTest extends ZKTestCase {
  private static final Logger LOG =
      LoggerFactory.getLogger(ZkClientUriDomainMappingHelperTest.class);
  private static final String HOSTPORT = "127.0.0.1:" + PortAssignment.unique();
  private static final String CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH = "/zookeeper/uri-domain-map";
  private static final int CONNECTION_TIMEOUT = 300000;
  private static final String[] MAPPING_PATHS = {
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH,
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/bar",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/bar/bar0",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/bar/bar1",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/foo",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/foo/foo1",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/foo/foo2",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/foo/bar1",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/helix-apps",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/helix-apps/workload",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/helix-apps/workload/helix-core",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/helix-apps/workload/helix-core/helix-controller",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/helix-apps/workload/helix-core/helix-rest",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/helix-mp",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/helix-mp/workload",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/helix-mp/workload/different-mp",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/helix-legacy",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/helix-legacy/urn:li:servicePrincipal(legacy;ei4;i001)"
  };

  private ZooKeeperServer zookeeperServer;
  private ZooKeeper zookeeperClientConnection;
  private ServerCnxnFactory serverCnxnFactory;

  @BeforeClass
  public static void registerBouncyCastle() {
    SpiffeAuthTestUtil.registerBouncyCastle();
  }

  @Before
  public void setUp() throws IOException, InterruptedException, KeeperException {
    LOG.info("Starting Zk...");
    zookeeperServer = new ZooKeeperServer(testBaseDir, testBaseDir, 3000);
    final int PORT = Integer.parseInt(HOSTPORT.split(":")[1]);
    serverCnxnFactory = ServerCnxnFactory.createFactory(PORT, -1);
    serverCnxnFactory.startup(zookeeperServer);

    LOG.info("Waiting for server startup");
    Assert.assertTrue("waiting for server being up ",
        ClientBase.waitForServerUp(HOSTPORT, CONNECTION_TIMEOUT));
    zookeeperClientConnection = new ZooKeeper(HOSTPORT, CONNECTION_TIMEOUT, DummyWatcher.INSTANCE);
  }

  @After
  public void cleanUp() throws InterruptedException, IOException, KeeperException {
    // Delete mapping znodes if they exist
    for (int i = MAPPING_PATHS.length - 1; i >= 0; i--) {
      if (zookeeperClientConnection.exists(MAPPING_PATHS[i], null) != null) {
        zookeeperClientConnection.delete(MAPPING_PATHS[i], -1);
      }
    }


    if (zookeeperClientConnection != null) {
      zookeeperClientConnection.close();
      zookeeperClientConnection = null;
    }
    if (serverCnxnFactory != null) {
      serverCnxnFactory.closeAll(ServerCnxn.DisconnectReason.SERVER_SHUTDOWN);
      serverCnxnFactory.shutdown();
      serverCnxnFactory = null;
    }
    if (zookeeperServer != null) {
      zookeeperServer.getZKDatabase().close();
      zookeeperServer.shutdown();
      zookeeperServer = null;
    }
    Assert.assertTrue("waiting for ZK server to shutdown",
        ClientBase.waitForServerDown(HOSTPORT, CONNECTION_TIMEOUT));
  }

  /**
   * Create a dummy mapping and verify that the helper correctly updates changes to the mapping
   * stored in ZNodes.
   *
   * The following mapping will be used
   * . (root)
   * └── _CLIENT_URI_DOMAIN_MAPPING (mapping root path)
   *     ├── bar (application domain)
   *     │   ├── bar0 (client URI)
   *     │   └── bar1 (client URI)
   *     └── foo (application domain)
   *         ├── foo1 (client URI)
   *         ├── foo2 (client URI)
   *         └── bar1 (client URI)
   */
  @Test
  public void testA_ZkClientUriDomainMappingHelper() throws KeeperException, InterruptedException {
    for (String path : MAPPING_PATHS) {
      zookeeperClientConnection
          .create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    ClientUriDomainMappingHelper helper = new ZkClientUriDomainMappingHelper(zookeeperServer);

    // For bar0, we should only get foo
    Assert.assertEquals(Collections.singleton("bar"), helper.getDomains("bar0"));

    // For bar1, we should get bar and foo
    Assert.assertEquals(new HashSet<>(Arrays.asList("bar", "foo")), helper.getDomains("bar1"));

    // Add a new application domain and add bar1 to it
    try {
      zookeeperClientConnection
          .create(CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/new", null, ZooDefs.Ids.OPEN_ACL_UNSAFE,
              CreateMode.PERSISTENT);
      zookeeperClientConnection.create(CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/new/bar1", null,
          ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

      // For bar1, we should get bar, foo, and new
      Assert.assertEquals(new HashSet<>(Arrays.asList("bar", "foo", "new")), helper.getDomains("bar1"));
    } finally {
      // Remove the application domain and bar1
      zookeeperClientConnection.delete(CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/new/bar1", -1);
      zookeeperClientConnection.delete(CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/new", -1);
    }

    // For bar1, we should get bar and foo
    Assert.assertEquals(new HashSet<>(Arrays.asList("bar", "foo")), helper.getDomains("bar1"));
  }

  /**
   * Verifies the helper recursively walks multi-level znode subtrees. SPIFFE ILM UIDs include
   * {@code /} separators (which can't appear in znode names), so they're encoded as a path of
   * nested znodes; only <b>leaves</b> are registered as keys. The mapping tree:
   * <pre>
   * helix-apps/workload/helix-core/helix-controller        → "workload/helix-core/helix-controller" → helix-apps
   * helix-apps/workload/helix-core/helix-rest              → "workload/helix-core/helix-rest"       → helix-apps
   * helix-mp/workload/different-mp                         → "workload/different-mp"                → helix-mp
   * helix-legacy/urn:li:servicePrincipal(legacy;ei4;i001)  → "urn:li:..."                           → helix-legacy
   * </pre>
   */
  @Test
  public void testA2_RecursiveZNodeWalkRegistersMultiLevelClientUris()
      throws KeeperException, InterruptedException {
    String[] paths = {
        CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH,
        CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/helix-apps",
        CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/helix-apps/workload",
        CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/helix-apps/workload/helix-core",
        CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/helix-apps/workload/helix-core/helix-controller",
        CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/helix-apps/workload/helix-core/helix-rest",
        CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/helix-mp",
        CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/helix-mp/workload",
        CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/helix-mp/workload/different-mp",
        CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/helix-legacy",
        CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/helix-legacy/urn:li:servicePrincipal(legacy;ei4;i001)"
    };
    for (String path : paths) {
      zookeeperClientConnection.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    ClientUriDomainMappingHelper helper = new ZkClientUriDomainMappingHelper(zookeeperServer);

    // App-level leaf: exact match
    Assert.assertEquals(Collections.singleton("helix-apps"),
        helper.getDomains("workload/helix-core/helix-controller"));

    // App-level leaf: walk-up from a deeper UID (e.g. app + tag)
    Assert.assertEquals(Collections.singleton("helix-apps"),
        helper.getDomains("workload/helix-core/helix-rest/ltx1-tag"));

    // MP-level leaf: walk-up grants the domain to any app under that MP
    Assert.assertEquals(Collections.singleton("helix-mp"),
        helper.getDomains("workload/different-mp/any-app/any-tag"));

    // Legacy URN entry resolves via exact match
    Assert.assertEquals(Collections.singleton("helix-legacy"),
        helper.getDomains("urn:li:servicePrincipal(legacy;ei4;i001)"));

    // No MP-level grant for helix-core, so an unregistered sibling app does NOT match
    Assert.assertEquals(Collections.emptySet(),
        helper.getDomains("workload/helix-core/unknown-app"));

    // Intermediate znode ("workload") is structural only — not registered as a 1-segment key
    Assert.assertEquals(Collections.emptySet(),
        helper.getDomains("workload/unrelated-mp/unrelated-app"));
  }

  /**
   * End-to-end: a real SPIFFE v2 client certificate flowing through
   * {@link X509ZNodeGroupAclProvider#handleAuthentication} resolves to the correct
   * {@code (x509, <domain>)} authInfo entry via the recursive znode mapping.
   */
  @Test
  public void testA3_SpiffeCertResolvesThroughZNodeMappingToDomainAuthInfo() throws Exception {
    String[] paths = {
        CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH,
        CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/helix-apps",
        CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/helix-apps/workload",
        CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/helix-apps/workload/helix-core",
        CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/helix-apps/workload/helix-core/helix-controller"
    };
    for (String path : paths) {
      zookeeperClientConnection.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    SpiffeAuthTestUtil.setSpiffeSystemProperties();
    try {
      X509Certificate cert = SpiffeAuthTestUtil.buildClientCertWithUriSans(
          "spiffe://prod.lipki/v2/workload/helix-core/helix-controller");

      X509ZNodeGroupAclProvider provider = new X509ZNodeGroupAclProvider(
          new SpiffeAuthTestUtil.AcceptAllTrustManager(), new SpiffeAuthTestUtil.NoopKeyManager());

      MockServerCnxn cnxn = new MockServerCnxn();
      cnxn.clientChain = new X509Certificate[]{cert};

      KeeperException.Code result = provider.handleAuthentication(
          new ServerAuthenticationProvider.ServerObjs(zookeeperServer, cnxn), null);

      Assert.assertEquals(KeeperException.Code.OK, result);

      boolean foundDomain = cnxn.getAuthInfo().stream()
          .anyMatch(id -> "x509".equals(id.getScheme()) && "helix-apps".equals(id.getId()));
      Assert.assertTrue(
          "Expected (x509, helix-apps) in authInfo; actual: " + cnxn.getAuthInfo(),
          foundDomain);
    } finally {
      SpiffeAuthTestUtil.clearSpiffeSystemProperties();
    }
  }

  @Test
  /**
   * Make sure the watcher installed while instantiate ZkClientUriDomainMappingHelper does not break
   * the functionality of getting watches
   */
  public void testB_GetWatches() {
    ClientUriDomainMappingHelper helper = new ZkClientUriDomainMappingHelper(zookeeperServer);
    WatchesReport report = zookeeperServer.getZKDatabase().getDataTree().getWatches();
    Assert.assertEquals(1, report.getPaths(0).size());
  }

  @Test
  public void testC_GetDomainsExactMatch() {
    ZkClientUriDomainMappingHelper helper = new ZkClientUriDomainMappingHelper(zookeeperServer);
    Map<String, Set<String>> mapping = new HashMap<>();
    mapping.put("workload/foo-mp/bar-app",
        new HashSet<>(Collections.singletonList("foo-domain")));
    setMapping(helper, mapping);

    Assert.assertEquals(Collections.singleton("foo-domain"),
        helper.getDomains("workload/foo-mp/bar-app"));
  }

  @Test
  public void testC_GetDomainsSegmentPrefixWalkUpSingleMatch() {
    ZkClientUriDomainMappingHelper helper = new ZkClientUriDomainMappingHelper(zookeeperServer);
    Map<String, Set<String>> mapping = new HashMap<>();
    mapping.put("workload/foo-mp", new HashSet<>(Collections.singletonList("foo-domain")));
    setMapping(helper, mapping);

    Assert.assertEquals(Collections.singleton("foo-domain"),
        helper.getDomains("workload/foo-mp/bar-app"));
  }

  @Test
  public void testC_GetDomainsSegmentPrefixWalkUpMultiSegmentUnion() {
    ZkClientUriDomainMappingHelper helper = new ZkClientUriDomainMappingHelper(zookeeperServer);
    Map<String, Set<String>> mapping = new HashMap<>();
    mapping.put("workload/foo-mp", new HashSet<>(Collections.singletonList("mp-domain")));
    mapping.put("workload/foo-mp/bar-app",
        new HashSet<>(Collections.singletonList("app-domain")));
    setMapping(helper, mapping);

    Assert.assertEquals(new HashSet<>(Arrays.asList("mp-domain", "app-domain")),
        helper.getDomains("workload/foo-mp/bar-app/some-tag"));
  }

  @Test
  public void testC_GetDomainsSegmentAlignmentIsStrict() {
    ZkClientUriDomainMappingHelper helper = new ZkClientUriDomainMappingHelper(zookeeperServer);
    Map<String, Set<String>> mapping = new HashMap<>();
    mapping.put("workload/foo-mp", new HashSet<>(Collections.singletonList("foo-domain")));
    setMapping(helper, mapping);

    Assert.assertEquals(Collections.emptySet(), helper.getDomains("workload/foo-mp-extra"));
  }

  @Test
  public void testC_GetDomainsUrnStyleNoWalkUp() {
    ZkClientUriDomainMappingHelper helper = new ZkClientUriDomainMappingHelper(zookeeperServer);
    Map<String, Set<String>> mapping = new HashMap<>();
    mapping.put("urn:li:servicePrincipal(bar;ei4;i001)",
        new HashSet<>(Collections.singletonList("bar-domain")));
    setMapping(helper, mapping);

    Assert.assertEquals(Collections.emptySet(),
        helper.getDomains("urn:li:servicePrincipal(foo;ei4;i001)"));
  }

  @Test
  public void testC_GetDomainsNullClientUri() {
    ZkClientUriDomainMappingHelper helper = new ZkClientUriDomainMappingHelper(zookeeperServer);
    Map<String, Set<String>> mapping = new HashMap<>();
    mapping.put("workload/foo-mp", new HashSet<>(Collections.singletonList("foo-domain")));
    setMapping(helper, mapping);

    Assert.assertEquals(Collections.emptySet(), helper.getDomains(null));
  }

  private static void setMapping(ZkClientUriDomainMappingHelper helper,
      Map<String, Set<String>> mapping) {
    helper.setClientUriToDomainNames(mapping);
  }
}
