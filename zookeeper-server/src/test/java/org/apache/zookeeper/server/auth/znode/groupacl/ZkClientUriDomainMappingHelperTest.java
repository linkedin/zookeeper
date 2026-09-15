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

import java.io.File;
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
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.MockServerCnxn;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.auth.ServerAuthenticationProvider;
import org.apache.zookeeper.server.auth.X509AuthenticationConfig;
import org.apache.zookeeper.server.auth.X509AuthenticationUtil.CertificateType;
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
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/helix-legacy/urn:li:servicePrincipal(legacy;ei4;i001)",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/helix-mp-grant",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/helix-mp-grant/application",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/helix-mp-grant/application/helix-core",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/broker-access",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/broker-access/servicePrincipal(kafka"
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
    File dataDir = ClientBase.createTmpDir();
    zookeeperServer = new ZooKeeperServer(dataDir, dataDir, 3000);
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

  /**
   * End-to-end: a SPIFFE v2 client cert whose principal is a multi-segment ILM UID resolves to
   * the correct {@code (x509, <domain>)} authInfo via the <em>segment-prefix walk-up</em> — the
   * operator registered only the MP-level leaf, not the full app-level path. This mirrors the
   * canonical LinkedIn ACL idiom (e.g. {@code acl-tool ... --spiffe "application/<mp>/*"}) and
   * ensures the prefix-walk-up is reachable from the production authentication path.
   */
  @Test
  public void testA4_SpiffeCertResolvesViaPrefixWalkUpToDomainAuthInfo() throws Exception {
    String[] paths = {
        CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH,
        CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/helix-mp-grant",
        CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/helix-mp-grant/application",
        CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/helix-mp-grant/application/helix-core"
    };
    for (String path : paths) {
      zookeeperClientConnection.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    SpiffeAuthTestUtil.setSpiffeSystemProperties();
    try {
      // Cert's path-after-/v2/ is "application/helix-core/helix-controller/ltx1-tag" (4 segments),
      // but the operator only registered the 2-segment leaf "application/helix-core". The walk-up
      // must hit that prefix and grant the helix-mp-grant domain.
      X509Certificate cert = SpiffeAuthTestUtil.buildClientCertWithUriSans(
          "spiffe://prod.lipki/v2/application/helix-core/helix-controller/ltx1-tag");

      X509ZNodeGroupAclProvider provider = new X509ZNodeGroupAclProvider(
          new SpiffeAuthTestUtil.AcceptAllTrustManager(), new SpiffeAuthTestUtil.NoopKeyManager());

      MockServerCnxn cnxn = new MockServerCnxn();
      cnxn.clientChain = new X509Certificate[]{cert};

      KeeperException.Code result = provider.handleAuthentication(
          new ServerAuthenticationProvider.ServerObjs(zookeeperServer, cnxn), null);

      Assert.assertEquals(KeeperException.Code.OK, result);

      boolean foundDomain = cnxn.getAuthInfo().stream()
          .anyMatch(id -> "x509".equals(id.getScheme()) && "helix-mp-grant".equals(id.getId()));
      Assert.assertTrue(
          "Expected (x509, helix-mp-grant) in authInfo via prefix walk-up; actual: " + cnxn.getAuthInfo(),
          foundDomain);
    } finally {
      SpiffeAuthTestUtil.clearSpiffeSystemProperties();
    }
  }

  @Test
  public void testA5_SpiffeApplicationsUseLegacyMappingAndObserveUpdates() throws Exception {
    for (String path : new String[] {
        CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH,
        CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/broker-access",
        CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/broker-access/servicePrincipal(kafka"
    }) {
      zookeeperClientConnection.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    X509ZNodeGroupAclProvider provider = new X509ZNodeGroupAclProvider(
        new SpiffeAuthTestUtil.AcceptAllTrustManager(), new SpiffeAuthTestUtil.NoopKeyManager());
    String mappingPath = CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/broker-access/servicePrincipal(kafka";
    for (String path : Arrays.asList(
        "/v1/wl/kafka", "/v1/application/example-mp/kafka",
        "/v2/application/example-mp/kafka", "/v2/application/example-mp/kafka/cluster-a")) {
      X509Certificate cert = SpiffeAuthTestUtil.buildClientCertWithUriSans("spiffe://example.org" + path);
      String clientId = path.equals("/v1/wl/kafka") ? "kafka" : path.substring("/v1/".length());
      MockServerCnxn cnxn = new MockServerCnxn();
      cnxn.clientChain = new X509Certificate[]{cert};
      ServerAuthenticationProvider.ServerObjs serverObjs =
          new ServerAuthenticationProvider.ServerObjs(zookeeperServer, cnxn);

      Assert.assertEquals(KeeperException.Code.OK, provider.handleAuthentication(serverObjs, null));
      Assert.assertTrue(cnxn.getAuthInfo().contains(new Id("x509", "broker-access")));
      Assert.assertEquals(clientId, cnxn.getX509ClientIdentity().getId());

      zookeeperClientConnection.delete(mappingPath, -1);
      Assert.assertEquals(KeeperException.Code.OK, provider.handleAuthentication(serverObjs, null));
      Assert.assertFalse(cnxn.getAuthInfo().contains(new Id("x509", "broker-access")));
      Assert.assertTrue(cnxn.getAuthInfo().contains(new Id("x509", clientId)));

      zookeeperClientConnection.create(mappingPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      Assert.assertEquals(KeeperException.Code.OK, provider.handleAuthentication(serverObjs, null));
      Assert.assertTrue(cnxn.getAuthInfo().contains(new Id("x509", "broker-access")));
    }
  }

  @Test
  public void testA6_LegacySanStillUsesOriginalMappingKey() throws Exception {
    for (String path : new String[] {
        CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH,
        CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/broker-access",
        CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/broker-access/servicePrincipal(kafka"
    }) {
      zookeeperClientConnection.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_TYPE, "SAN");
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_TYPE, "6");
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_REGEX,
        "^urn:li:servicePrincipal\\(");
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_REGEX,
        "^urn:li:([a-z]+Principal\\([^;%:]+)");
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_MATCHER_GROUP_INDEX, "1");
    X509AuthenticationConfig.reset();
    try {
      X509Certificate cert = SpiffeAuthTestUtil.buildClientCertWithUriSans(
          "urn:li:servicePrincipal(kafka;region1;instance1)");
      X509ZNodeGroupAclProvider provider = new X509ZNodeGroupAclProvider(
          new SpiffeAuthTestUtil.AcceptAllTrustManager(), new SpiffeAuthTestUtil.NoopKeyManager());
      MockServerCnxn cnxn = new MockServerCnxn();
      cnxn.clientChain = new X509Certificate[]{cert};

      Assert.assertEquals(KeeperException.Code.OK, provider.handleAuthentication(
          new ServerAuthenticationProvider.ServerObjs(zookeeperServer, cnxn), null));
      Assert.assertTrue(cnxn.getAuthInfo().contains(new Id("x509", "broker-access")));
    } finally {
      System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_TYPE);
      System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_REGEX);
      System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_REGEX);
      System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_MATCHER_GROUP_INDEX);
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

  @Test
  public void testD_SpiffeWorkloadMatchesLegacyServicePrincipalNames() {
    ZkClientUriDomainMappingHelper helper = new ZkClientUriDomainMappingHelper(zookeeperServer);
    Map<String, Set<String>> mapping = new HashMap<>();
    mapping.put("servicePrincipal(kafka", Collections.singleton("truncated-domain"));
    mapping.put("servicePrincipal(kafka)", Collections.singleton("closed-domain"));
    mapping.put("urn:li:servicePrincipal(kafka;region1;instance1)", Collections.singleton("urn-domain"));
    setMapping(helper, mapping);

    Assert.assertEquals(new HashSet<>(Arrays.asList("truncated-domain", "closed-domain", "urn-domain")),
        helper.getDomains(CertificateType.SPIFFE_V1_WL, "kafka"));
    for (CertificateType type : Arrays.asList(
        CertificateType.SPIFFE_V1_WORKLOAD, CertificateType.SPIFFE_V2)) {
      for (String clientId : Arrays.asList("application/example-mp/kafka", "application/example-mp/kafka/cluster-a")) {
        Assert.assertEquals(new HashSet<>(Arrays.asList("truncated-domain", "closed-domain", "urn-domain")),
            helper.getDomains(type, clientId));
      }
    }
    Assert.assertEquals(Collections.singleton("truncated-domain"),
        helper.getDomains(CertificateType.LEGACY_SAN, "servicePrincipal(kafka"));
    Assert.assertEquals(Collections.singleton("urn-domain"),
        helper.getDomains("urn:li:servicePrincipal(kafka;region1;instance1)"));
  }

  @Test
  public void testD_LegacyAliasesRequireSpiffeWorkloadType() {
    ZkClientUriDomainMappingHelper helper = new ZkClientUriDomainMappingHelper(zookeeperServer);
    setMapping(helper, Collections.singletonMap("servicePrincipal(kafka", Collections.singleton("broker-access")));

    for (CertificateType type : CertificateType.values()) {
      if (type != CertificateType.SPIFFE_V1_WL) {
        Assert.assertEquals(type.name(), Collections.emptySet(), helper.getDomains(type, "kafka"));
      }
    }
    Assert.assertEquals(Collections.emptySet(), helper.getDomains("kafka"));
    Assert.assertEquals(Collections.emptySet(), helper.getDomains(CertificateType.SPIFFE_V1_WL, null));
  }

  @Test
  public void testD_ExactIdentityMappingOverridesLegacyAlias() {
    ZkClientUriDomainMappingHelper helper = new ZkClientUriDomainMappingHelper(zookeeperServer);
    Map<String, Set<String>> mapping = new HashMap<>();
    mapping.put("kafka", Collections.singleton("explicit-domain"));
    mapping.put("servicePrincipal(kafka", Collections.singleton("legacy-domain"));
    mapping.put("application/example-mp", Collections.singleton("prefix-domain"));
    mapping.put("application/example-mp/kafka", Collections.singleton("exact-domain"));
    setMapping(helper, mapping);

    Assert.assertEquals(Collections.singleton("explicit-domain"),
        helper.getDomains(CertificateType.SPIFFE_V1_WL, "kafka"));
    for (CertificateType type : Arrays.asList(
        CertificateType.SPIFFE_V1_WORKLOAD, CertificateType.SPIFFE_V2)) {
      Assert.assertEquals(Collections.singleton("exact-domain"),
          helper.getDomains(type, "application/example-mp/kafka"));
    }
  }

  @Test
  public void testD_TypedMultiSegmentLookupRetainsPrefixMatching() {
    ZkClientUriDomainMappingHelper helper = new ZkClientUriDomainMappingHelper(zookeeperServer);
    Map<String, Set<String>> mapping = new HashMap<>();
    mapping.put("servicePrincipal(kafka", Collections.singleton("legacy-domain"));
    mapping.put("application/example-mp", Collections.singleton("path-domain"));
    setMapping(helper, mapping);

    for (CertificateType type : Arrays.asList(
        CertificateType.SPIFFE_V1_WORKLOAD, CertificateType.SPIFFE_V2)) {
      Assert.assertEquals(Collections.singleton("path-domain"),
          helper.getDomains(type, "application/example-mp/kafka"));
      Assert.assertEquals(Collections.singleton("legacy-domain"),
          helper.getDomains(type, "application/unrelated-mp/kafka"));
      Assert.assertEquals(Collections.emptySet(), helper.getDomains(type, "group/kafka"));
    }
    mapping.put("application/example-mp/kafka", Collections.singleton("app-domain"));
    for (CertificateType type : Arrays.asList(
        CertificateType.SPIFFE_V1_WORKLOAD, CertificateType.SPIFFE_V2)) {
      Assert.assertEquals(new HashSet<>(Arrays.asList("path-domain", "app-domain")),
          helper.getDomains(type, "application/example-mp/kafka/cluster-a"));
    }
  }

  @Test
  public void testD_EmptyPrefixMappingDoesNotFallBackToLegacy() {
    ZkClientUriDomainMappingHelper helper = new ZkClientUriDomainMappingHelper(zookeeperServer);
    Map<String, Set<String>> mapping = new HashMap<>();
    mapping.put("servicePrincipal(kafka", Collections.singleton("legacy-domain"));
    mapping.put("application/example-mp", Collections.emptySet());
    setMapping(helper, mapping);

    for (CertificateType type : Arrays.asList(
        CertificateType.SPIFFE_V1_WORKLOAD, CertificateType.SPIFFE_V2)) {
      Assert.assertEquals(Collections.emptySet(), helper.getDomains(type, "application/example-mp/kafka"));
    }
  }

  @Test
  public void testD_ApplicationAliasesRequireCompleteApplicationPath() {
    ZkClientUriDomainMappingHelper helper = new ZkClientUriDomainMappingHelper(zookeeperServer);
    setMapping(helper, Collections.singletonMap("servicePrincipal(kafka", Collections.singleton("broker-access")));

    for (CertificateType type : Arrays.asList(
        CertificateType.SPIFFE_V1_WORKLOAD, CertificateType.SPIFFE_V2)) {
      for (String clientId : Arrays.asList(
          "application", "application/kafka", "application/kafka/", "application//kafka",
          "application/example-mp/kafka/", "application/example-mp/kafka//cluster-a",
          "application/example-mp/kafka/tag/extra", "application/kafka/other",
          "application/example-mp/other/kafka", "application/example-mp/kafka-extra",
          "application/example-mp/Kafka", "application/example-mp/%6bafka",
          "Application/example-mp/kafka", "workload/example-mp/kafka",
          "airflow/kafka", "group/kafka", "user/kafka", "group/application/example-mp/kafka")) {
        Assert.assertEquals(type + ": " + clientId, Collections.emptySet(), helper.getDomains(type, clientId));
      }
    }
  }

  @Test
  public void testD_ApplicationAliasesRequireSpiffeCertificateType() {
    ZkClientUriDomainMappingHelper helper = new ZkClientUriDomainMappingHelper(zookeeperServer);
    setMapping(helper, Collections.singletonMap("servicePrincipal(kafka", Collections.singleton("broker-access")));

    for (CertificateType type : Arrays.asList(
        CertificateType.SPIFFE_V1_WL, CertificateType.LEGACY_SAN, CertificateType.SUBJECT_DN)) {
      Assert.assertEquals(type.name(), Collections.emptySet(),
          helper.getDomains(type, "application/example-mp/kafka"));
    }
    Assert.assertEquals(Collections.emptySet(), helper.getDomains("application/example-mp/kafka"));
  }

  @Test
  public void testD_OtherPrincipalKindsAndMalformedNamesAreNotAliases() {
    ZkClientUriDomainMappingHelper helper = new ZkClientUriDomainMappingHelper(zookeeperServer);
    Map<String, Set<String>> mapping = new HashMap<>();
    for (String name : Arrays.asList(
        "userPrincipal(kafka",
        "groupPrincipal(kafka",
        "servicePrincipalMetadata(kafka)",
        "urn:li:userPrincipal(kafka;region1;instance1)",
        "urn:li:groupPrincipal(kafka;region1;instance1)",
        "urn:li:servicePrincipalMetadata(kafka;region1;instance1)",
        "servicePrincipal(kafka-extra",
        "servicePrincipal(Kafka",
        "servicePrincipal(%6bafka",
        "servicePrincipal(kafka)extra",
        "servicePrincipal(kafka;region1;instance1",
        "nested/servicePrincipal(kafka")) {
      mapping.put(name, Collections.singleton("unrelated-domain"));
    }
    setMapping(helper, mapping);

    Assert.assertEquals(Collections.emptySet(), helper.getDomains(CertificateType.SPIFFE_V1_WL, "kafka"));
    for (CertificateType type : Arrays.asList(
        CertificateType.SPIFFE_V1_WORKLOAD, CertificateType.SPIFFE_V2)) {
      Assert.assertEquals(Collections.emptySet(), helper.getDomains(type, "application/example-mp/kafka"));
    }
    Assert.assertEquals(Collections.singleton("unrelated-domain"),
        helper.getDomains(CertificateType.LEGACY_SAN, "userPrincipal(kafka"));
    Assert.assertEquals(Collections.singleton("unrelated-domain"),
        helper.getDomains(CertificateType.LEGACY_SAN, "groupPrincipal(kafka"));
  }

  private static void setMapping(ZkClientUriDomainMappingHelper helper,
      Map<String, Set<String>> mapping) {
    helper.setClientUriToDomainNames(mapping);
  }
}
