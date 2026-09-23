/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.auth.znode.groupacl;

import java.net.InetSocketAddress;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.common.SpiffeAuthTestUtil;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.MockServerCnxn;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.PrepRequestProcessor;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.auth.ServerAuthenticationProvider;
import org.apache.zookeeper.server.auth.X509AuthenticationConfig;
import org.apache.zookeeper.test.ClientBase;
import org.apache.zookeeper.test.X509AuthTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class X509ZNodeGroupAclProviderTest extends ZKTestCase {
  private static final Logger LOG = LoggerFactory.getLogger(X509ZNodeGroupAclProviderTest.class);
  private static final String HOSTPORT = "127.0.0.1:" + PortAssignment.unique();
  private static X509AuthTest.TestCertificate domainXCert;
  private static X509AuthTest.TestCertificate superCert;
  private static X509AuthTest.TestCertificate superCert2;
  private static X509AuthTest.TestCertificate unknownCert;
  private static X509AuthTest.TestCertificate crossDomainCert;
  private static final String CLIENT_CERT_ID_SAN_MATCH_TYPE = "6";
  private static final String SCHEME = "x509";
  private static ZooKeeperServer zks;
  private TestNIOServerCnxnFactory serverCnxnFactory;
  private ZooKeeper admin;
  private static final String AUTH_PROVIDER_PROPERTY_NAME = "zookeeper.authProvider.x509";
  private static final String CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH = "/zookeeper/uri-domain-map";
  private static final String[] MAPPING_PATHS = {CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH,
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/CrossDomain",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/CrossDomain/CrossDomainUser",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/DomainX",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/DomainX/DomainXUser",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/DomainY",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/DomainY/DomainYUser"};
  private static final Map<String, String> SYSTEM_PROPERTIES = new HashMap<>();
    static {
      SYSTEM_PROPERTIES.put(X509AuthenticationConfig.ZOOKEEPER_ZNODEGROUPACL_SUPERUSER_ID, "SuperUser,SuperUser2");
      SYSTEM_PROPERTIES.put(X509AuthenticationConfig.SSL_X509_LEGACY_SUPER_USER_COMPATIBILITY_ENABLED, "false");
      SYSTEM_PROPERTIES.put(X509AuthenticationConfig.SET_X509_CLIENT_ID_AS_ACL, "false");
      SYSTEM_PROPERTIES.put("zookeeper.ssl.keyManager", "org.apache.zookeeper.test.X509AuthTest.TestKeyManager");
      SYSTEM_PROPERTIES.put("zookeeper.ssl.trustManager", "org.apache.zookeeper.test.X509AuthTest.TestTrustManager");
      SYSTEM_PROPERTIES.put(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_TYPE, X509AuthenticationConfig.SUBJECT_ALTERNATIVE_NAME_SHORT);
      SYSTEM_PROPERTIES.put(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_TYPE, CLIENT_CERT_ID_SAN_MATCH_TYPE);
      SYSTEM_PROPERTIES.put(AUTH_PROVIDER_PROPERTY_NAME, X509ZNodeGroupAclProvider.class.getCanonicalName());
      SYSTEM_PROPERTIES.put(X509AuthenticationConfig.CROSS_DOMAIN_ACCESS_DOMAIN_NAME, "CrossDomain");
    }

  @Before
  public void setUp() throws Exception {
    for (Map.Entry<String, String> property : SYSTEM_PROPERTIES.entrySet()) {
      System.setProperty(property.getKey(), property.getValue());
    }
    LOG.info("Starting Zk...");
    zks = new ZooKeeperServer(testBaseDir, testBaseDir, 3000);
    final int PORT = Integer.parseInt(HOSTPORT.split(":")[1]);
    serverCnxnFactory = new TestNIOServerCnxnFactory();
    serverCnxnFactory.configure(new InetSocketAddress(PORT), -1, -1);
    serverCnxnFactory.startup(zks);
    LOG.info("Waiting for server startup");
    Assert.assertTrue("waiting for server being up ", ClientBase.waitForServerUp(HOSTPORT, 300000));
    admin = ClientBase.createZKClient(HOSTPORT);
    try {
      ZKUtil.deleteRecursive(admin, CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH);
    } catch (Exception ignored) {
    }

    // Create test client certificates
    domainXCert = new X509AuthTest.TestCertificate("CLIENT", "DomainXUser");
    superCert = new X509AuthTest.TestCertificate("SUPER", "SuperUser");
    superCert2 = new X509AuthTest.TestCertificate("SUPER", "SuperUser2");
    unknownCert = new X509AuthTest.TestCertificate("UNKNOWN", "UnknownUser");
    crossDomainCert = new X509AuthTest.TestCertificate("CLIENT", "CrossDomainUser");

    // Create Client URI - domain mapping znodes
    for (String path : MAPPING_PATHS) {
      // Create ACL metadata
      admin.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
  }

  @After
  public void cleanUp() throws InterruptedException, KeeperException {
    LOG.info("X509ZNodeGroupAclProviderTest::cleanUp() called!");
    X509AuthenticationConfig.reset();
    ZKUtil.deleteRecursive(admin, CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH);
    zks.shutdown();
    admin.close();
    serverCnxnFactory.shutdown();
    for (Map.Entry<String, String> property : SYSTEM_PROPERTIES.entrySet()) {
      System.clearProperty(property.getKey());
    }
  }

  @Test
  public void testUntrustedClient() {
    X509ZNodeGroupAclProvider provider = createProvider(domainXCert);
    MockServerCnxn cnxn = new MockServerCnxn();
    cnxn.clientChain = new X509Certificate[]{unknownCert};
    Assert.assertEquals(KeeperException.Code.AUTHFAILED, provider
        .handleAuthentication(new ServerAuthenticationProvider.ServerObjs(zks, cnxn), new byte[0]));
  }

  @Test
  public void testAuthorizedClient() {
    X509ZNodeGroupAclProvider provider = createProvider(domainXCert);
    MockServerCnxn cnxn = new MockServerCnxn();
    cnxn.clientChain = new X509Certificate[]{domainXCert};
    Assert.assertEquals(KeeperException.Code.OK, provider
        .handleAuthentication(new ServerAuthenticationProvider.ServerObjs(zks, cnxn), new byte[0]));
    List<Id> authInfo = cnxn.getAuthInfo();
    Assert.assertEquals(1, authInfo.size());
    Assert.assertEquals(SCHEME, authInfo.get(0).getScheme());
    Assert.assertEquals("DomainX", authInfo.get(0).getId());
  }

  @Test
  public void testUnauthorizedClient() {
    X509ZNodeGroupAclProvider provider = createProvider(unknownCert);
    MockServerCnxn cnxn = new MockServerCnxn();
    cnxn.clientChain = new X509Certificate[]{unknownCert};
    Assert.assertEquals(KeeperException.Code.OK, provider
        .handleAuthentication(new ServerAuthenticationProvider.ServerObjs(zks, cnxn), new byte[0]));
    List<Id> authInfo = cnxn.getAuthInfo();
    Assert.assertEquals(1, authInfo.size());
    Assert.assertEquals(SCHEME, authInfo.get(0).getScheme());
    Assert.assertEquals("UnknownUser", authInfo.get(0).getId());
  }

  @Test
  public void testSuperUser() {
    // Belong to super user domain
    X509ZNodeGroupAclProvider provider = createProvider(crossDomainCert);
    MockServerCnxn cnxn = new MockServerCnxn();
    cnxn.clientChain = new X509Certificate[]{crossDomainCert};
    Assert.assertEquals(KeeperException.Code.OK, provider
        .handleAuthentication(new ServerAuthenticationProvider.ServerObjs(zks, cnxn), new byte[0]));
    List<Id> authInfo = cnxn.getAuthInfo();
    Assert.assertEquals(1, authInfo.size());
    Assert.assertEquals("super", authInfo.get(0).getScheme());
    Assert.assertEquals("CrossDomain", authInfo.get(0).getId());

    // Directly set multiple service principals in config as super user
    // 1st super user
    provider = createProvider(superCert);
    cnxn = new MockServerCnxn();
    cnxn.clientChain = new X509Certificate[]{superCert};
    Assert.assertEquals(KeeperException.Code.OK, provider
        .handleAuthentication(new ServerAuthenticationProvider.ServerObjs(zks, cnxn), new byte[0]));
    authInfo = cnxn.getAuthInfo();
    Assert.assertEquals(1, authInfo.size());
    Assert.assertEquals("super", authInfo.get(0).getScheme());
    Assert.assertEquals("SuperUser", authInfo.get(0).getId());

    // 2nd super user
    provider = createProvider(superCert2);
    cnxn = new MockServerCnxn();
    cnxn.clientChain = new X509Certificate[]{superCert2};
    Assert.assertEquals(KeeperException.Code.OK, provider
        .handleAuthentication(new ServerAuthenticationProvider.ServerObjs(zks, cnxn), new byte[0]));
    authInfo = cnxn.getAuthInfo();
    Assert.assertEquals(1, authInfo.size());
    Assert.assertEquals("super", authInfo.get(0).getScheme());
    Assert.assertEquals("SuperUser2", authInfo.get(0).getId());
  }

  @Test
  public void testSpiffeSuperUserCompatibilityRequiresOptIn() throws Exception {
    String configuredId = "servicePrincipal(kafka";
    System.setProperty(X509AuthenticationConfig.ZOOKEEPER_ZNODEGROUPACL_SUPERUSER_ID, configuredId);
    for (String path : Arrays.asList(
        "/v1/wl/kafka", "/v1/application/example-mp/kafka", "/v2/application/example-mp/kafka/blue")) {
      String clientId = path.equals("/v1/wl/kafka") ? "kafka" : path.substring("/v1/".length());
      System.clearProperty(X509AuthenticationConfig.SSL_X509_LEGACY_SUPER_USER_COMPATIBILITY_ENABLED);
      MockServerCnxn normal = authenticateSpiffe(path);
      Assert.assertEquals(Collections.singletonList(new Id("x509", clientId)), normal.getAuthInfo());

      System.setProperty(X509AuthenticationConfig.SSL_X509_LEGACY_SUPER_USER_COMPATIBILITY_ENABLED, "true");
      MockServerCnxn superUser = authenticateSpiffe(path);
      Assert.assertEquals(clientId, superUser.getX509ClientIdentity().getId());
      Assert.assertEquals(Collections.singletonList(new Id("super", configuredId)), superUser.getAuthInfo());
      zks.checkACL(superUser, Collections.singletonList(new ACL(ZooDefs.Perms.READ, new Id("x509", "unrelated"))),
          ZooDefs.Perms.ADMIN, superUser.getAuthInfo(), "/protected", null);
    }
  }

  @Test
  public void testBareSuperUserConfigurationSupportsSpiffeApplications() throws Exception {
    System.setProperty(X509AuthenticationConfig.ZOOKEEPER_ZNODEGROUPACL_SUPERUSER_ID, "kafka");
    for (String path : Arrays.asList(
        "/v1/application/example-mp/kafka", "/v2/application/example-mp/kafka/blue")) {
      String clientId = path.substring("/v1/".length());
      System.setProperty(X509AuthenticationConfig.SSL_X509_LEGACY_SUPER_USER_COMPATIBILITY_ENABLED, "false");
      Assert.assertEquals(Collections.singletonList(new Id("x509", clientId)), authenticateSpiffe(path).getAuthInfo());

      System.setProperty(X509AuthenticationConfig.SSL_X509_LEGACY_SUPER_USER_COMPATIBILITY_ENABLED, "true");
      MockServerCnxn superUser = authenticateSpiffe(path);
      Assert.assertEquals(Collections.singletonList(new Id("super", "kafka")), superUser.getAuthInfo());
      Assert.assertEquals(clientId, superUser.getX509ClientIdentity().getId());
    }
  }

  @Test
  public void testExactSuperUserIdWinsBeforeCompatibleMarkers() throws Exception {
    String legacyId = "servicePrincipal(zookeeper";
    String applicationId = "application/example-mp/zookeeper";
    System.setProperty(X509AuthenticationConfig.ZOOKEEPER_ZNODEGROUPACL_SUPERUSER_ID,
        legacyId + ",zookeeper," + applicationId);
    for (String enabled : Arrays.asList("false", "true")) {
      System.setProperty(X509AuthenticationConfig.SSL_X509_LEGACY_SUPER_USER_COMPATIBILITY_ENABLED, enabled);
      Assert.assertEquals(Collections.singletonList(new Id("super", "zookeeper")),
          authenticateSpiffe("/v1/wl/zookeeper").getAuthInfo());
      Assert.assertEquals(Collections.singletonList(new Id("super", applicationId)),
          authenticateSpiffe("/v2/" + applicationId).getAuthInfo());
    }
    System.setProperty(X509AuthenticationConfig.SSL_X509_LEGACY_SUPER_USER_COMPATIBILITY_ENABLED, "true");
    for (String configuredIds : Arrays.asList(legacyId + ")," + legacyId, legacyId + "," + legacyId + ")")) {
      System.setProperty(X509AuthenticationConfig.ZOOKEEPER_ZNODEGROUPACL_SUPERUSER_ID, configuredIds);
      Assert.assertEquals(Collections.singletonList(new Id("super", legacyId)),
          authenticateSpiffe("/v1/wl/zookeeper").getAuthInfo());
    }
  }

  @Test
  public void testSuperUserCompatibilityRejectsOtherIdentityTypes() throws Exception {
    System.setProperty(X509AuthenticationConfig.SSL_X509_LEGACY_SUPER_USER_COMPATIBILITY_ENABLED, "true");
    System.setProperty(X509AuthenticationConfig.ZOOKEEPER_ZNODEGROUPACL_SUPERUSER_ID, "servicePrincipal(kafka");
    for (String path : Arrays.asList(
        "/v2/kafka", "/v2/user/kafka", "/v2/group/kafka", "/v1/airflow/kafka",
        "/v2/workload/example-mp/kafka", "/v2/application/kafka",
        "/v2/application//kafka", "/v2/application/example-mp/other/kafka")) {
      Assert.assertFalse(authenticateSpiffe(path).getAuthInfo().stream()
          .anyMatch(id -> id.getScheme().equals("super")));
    }
    for (String configuredId : Arrays.asList(
        "other", "Kafka", "kafka)", "kafka;instance", "application/other-mp/kafka",
        "servicePrincipal(other", "servicePrincipal(Kafka", "userPrincipal(kafka",
        "groupPrincipal(kafka", "servicePrincipalMetadata(kafka)")) {
      System.setProperty(X509AuthenticationConfig.ZOOKEEPER_ZNODEGROUPACL_SUPERUSER_ID, configuredId);
      Assert.assertFalse(authenticateSpiffe("/v2/application/example-mp/kafka").getAuthInfo().stream()
          .anyMatch(id -> id.getScheme().equals("super")));
    }
    for (String structuredId : Arrays.asList("CN=admin", "urn:example:admin", "_kafka", "-kafka", ".kafka")) {
      System.setProperty(X509AuthenticationConfig.ZOOKEEPER_ZNODEGROUPACL_SUPERUSER_ID, structuredId);
      MockServerCnxn cnxn = authenticateSpiffe("/v2/application/example-mp/" + structuredId);
      Assert.assertEquals("application/example-mp/" + structuredId, cnxn.getX509ClientIdentity().getId());
      Assert.assertFalse(cnxn.getAuthInfo().stream().anyMatch(id -> id.getScheme().equals("super")));
    }
    System.setProperty(X509AuthenticationConfig.ZOOKEEPER_ZNODEGROUPACL_SUPERUSER_ID, "kafka");
    X509AuthenticationConfig.reset();
    X509AuthTest.TestCertificate legacyCert = new X509AuthTest.TestCertificate("CLIENT", "servicePrincipal(kafka");
    MockServerCnxn legacy = new MockServerCnxn();
    legacy.clientChain = new X509Certificate[]{legacyCert};
    Assert.assertEquals(KeeperException.Code.OK, createProvider(legacyCert).handleAuthentication(
        new ServerAuthenticationProvider.ServerObjs(zks, legacy), null));
    Assert.assertEquals(Collections.singletonList(new Id("x509", "servicePrincipal(kafka")), legacy.getAuthInfo());
  }

  @Test
  public void testStructuredSuperUserIdsKeepExactLegacyMatches() throws Exception {
    System.setProperty(X509AuthenticationConfig.SSL_X509_LEGACY_SUPER_USER_COMPATIBILITY_ENABLED, "true");
    for (String clientId : Arrays.asList("CN=admin", "urn:example:admin")) {
      System.setProperty(X509AuthenticationConfig.ZOOKEEPER_ZNODEGROUPACL_SUPERUSER_ID, clientId);
      X509AuthenticationConfig.reset();
      X509AuthTest.TestCertificate cert = new X509AuthTest.TestCertificate("CLIENT", clientId);
      MockServerCnxn cnxn = new MockServerCnxn();
      cnxn.clientChain = new X509Certificate[]{cert};
      Assert.assertEquals(KeeperException.Code.OK, createProvider(cert).handleAuthentication(
          new ServerAuthenticationProvider.ServerObjs(zks, cnxn), null));
      Assert.assertEquals(Collections.singletonList(new Id("super", clientId)), cnxn.getAuthInfo());
    }
  }

  @Test
  public void testCompatibleSuperUserRetainsExplicitAclPolicy() throws Exception {
    System.setProperty(X509AuthenticationConfig.SET_X509_CLIENT_ID_AS_ACL, "true");
    List<ACL> requested = Collections.singletonList(new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE));
    for (String configuredId : Arrays.asList("kafka", "servicePrincipal(kafka")) {
      System.setProperty(X509AuthenticationConfig.ZOOKEEPER_ZNODEGROUPACL_SUPERUSER_ID, configuredId);
      System.setProperty(X509AuthenticationConfig.SSL_X509_LEGACY_SUPER_USER_COMPATIBILITY_ENABLED, "false");
      admin.create(CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/CrossDomain/" + configuredId,
          null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

      MockServerCnxn crossDomain = authenticateSpiffe("/v2/application/example-mp/kafka");
      Assert.assertEquals(Collections.singletonList(new Id("super", "CrossDomain")), crossDomain.getAuthInfo());
      Assert.assertEquals(Collections.singletonList(new ACL(ZooDefs.Perms.ALL, new Id("x509", "CrossDomain"))),
          PrepRequestProcessor.fixupACL("/created", crossDomain.getAuthInfo(), requested));

      System.setProperty(X509AuthenticationConfig.SSL_X509_LEGACY_SUPER_USER_COMPATIBILITY_ENABLED, "true");
      MockServerCnxn explicitSuperUser = authenticateSpiffe("/v2/application/example-mp/kafka");
      Assert.assertEquals(Collections.singletonList(new Id("super", configuredId)), explicitSuperUser.getAuthInfo());
      Assert.assertEquals("application/example-mp/kafka", explicitSuperUser.getX509ClientIdentity().getId());
      Assert.assertEquals(requested, PrepRequestProcessor.fixupACL("/created", explicitSuperUser.getAuthInfo(), requested));
    }
  }

  @Test
  public void testSuperUserCompatibilityDoesNotUseMappedDomainAsIdentity() throws Exception {
    System.setProperty(X509AuthenticationConfig.SSL_X509_LEGACY_SUPER_USER_COMPATIBILITY_ENABLED, "true");
    admin.create(CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/DomainX/servicePrincipal(kafka",
        null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    for (String configuredId : Arrays.asList("DomainX", "servicePrincipal(DomainX")) {
      System.setProperty(X509AuthenticationConfig.ZOOKEEPER_ZNODEGROUPACL_SUPERUSER_ID, configuredId);
      MockServerCnxn cnxn = authenticateSpiffe("/v2/application/example-mp/kafka");
      Assert.assertEquals(Collections.singletonList(new Id("x509", "DomainX")), cnxn.getAuthInfo());
    }
  }

  @Test
  public void testAuthInfoAutoUpdate() throws InterruptedException, KeeperException {
    String clientId = "DomainZUser";
    X509AuthTest.TestCertificate domainZCert = new X509AuthTest.TestCertificate("CLIENT", clientId);
    String oldDomain = CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/DomainZ";
    String newDomain = CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/DomainZN";

    admin.create(oldDomain, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    admin.create(oldDomain + "/" + clientId, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

    // Test with original domain info
    X509ZNodeGroupAclProvider provider = createProvider(domainZCert);
    MockServerCnxn cnxn = new MockServerCnxn();

    // Inject the new connection to factory so it's AuthInfo will be auto-refreshed.
    serverCnxnFactory.getClients().add(cnxn);

    // Check the original status.
    cnxn.clientChain = new X509Certificate[]{domainZCert};
    Assert.assertEquals(KeeperException.Code.OK, provider
        .handleAuthentication(new ServerAuthenticationProvider.ServerObjs(zks, cnxn), new byte[0]));
    List<Id> authInfo = cnxn.getAuthInfo();
    Assert.assertEquals(1, authInfo.size());
    Assert.assertEquals(SCHEME, authInfo.get(0).getScheme());
    Assert.assertEquals("DomainZ", authInfo.get(0).getId());

    // Add new domain info.
    admin.create(newDomain, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    admin.create(newDomain + "/" + clientId, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    waitFor("AuthInfo is not updated after new domain created.", () -> {
      return cnxn.getAuthInfo().size() == 2;
    }, 3);

    // Remove the original domain info
    admin.delete(oldDomain + "/" + clientId, -1);
    admin.delete(oldDomain, -1);
    waitFor("AuthInfo is not updated after old domain removed.", () -> {
      List<Id> newAuthInfo = cnxn.getAuthInfo();
      return 1 == newAuthInfo.size()
          && SCHEME.equals(newAuthInfo.get(0).getScheme())
          && newAuthInfo.get(0).getId().equals("DomainZN");
    }, 3);
  }

  @Test
  public void testConnectionFiltering() {
    // Single domain user
    System.setProperty(X509AuthenticationConfig.DEDICATED_DOMAIN, "DomainX");
    X509ZNodeGroupAclProvider provider = createProvider(domainXCert);
    MockServerCnxn cnxn = new MockServerCnxn();
    cnxn.clientChain = new X509Certificate[]{domainXCert};
    Assert.assertEquals(KeeperException.Code.OK, provider
        .handleAuthentication(new ServerAuthenticationProvider.ServerObjs(zks, cnxn), new byte[0]));
    List<Id> authInfo = cnxn.getAuthInfo();
    Assert.assertEquals(1, authInfo.size());
    Assert.assertEquals(SCHEME, authInfo.get(0).getScheme());
    Assert.assertEquals("DomainXUser", authInfo.get(0).getId());

    // Non-authorized user (connection filtering enforced)
    ClosableMockServerCnxn closableMockServerCnxn = new ClosableMockServerCnxn();
    closableMockServerCnxn.clientChain = new X509Certificate[]{domainXCert};
    System.clearProperty(X509AuthenticationConfig.DEDICATED_DOMAIN);
    System.setProperty(X509AuthenticationConfig.DEDICATED_DOMAIN, "DomainY");
    X509AuthenticationConfig.reset();
    provider = createProvider(domainXCert);
    provider
        .handleAuthentication(new ServerAuthenticationProvider.ServerObjs(zks, closableMockServerCnxn), new byte[0]);
    Assert.assertTrue(closableMockServerCnxn.isClosed());
    System.clearProperty(X509AuthenticationConfig.DEDICATED_DOMAIN);

    // Non-authorized user (connection filtering not enforced)
    closableMockServerCnxn = new ClosableMockServerCnxn();
    closableMockServerCnxn.clientChain = new X509Certificate[]{domainXCert};
    System.setProperty(X509AuthenticationConfig.DEDICATED_DOMAIN, "DomainY");
    System.setProperty(X509AuthenticationConfig.ENFORCE_DEDICATED_DOMAIN, "false");
    X509AuthenticationConfig.reset();
    provider = createProvider(domainXCert);
    provider
        .handleAuthentication(new ServerAuthenticationProvider.ServerObjs(zks, closableMockServerCnxn), new byte[0]);
    Assert.assertFalse(closableMockServerCnxn.isClosed());
    System.clearProperty(X509AuthenticationConfig.DEDICATED_DOMAIN);
    System.clearProperty(X509AuthenticationConfig.ENFORCE_DEDICATED_DOMAIN);

    // Super user
    provider = createProvider(superCert);
    cnxn = new MockServerCnxn();
    cnxn.clientChain = new X509Certificate[]{superCert};
    Assert.assertEquals(KeeperException.Code.OK, provider
        .handleAuthentication(new ServerAuthenticationProvider.ServerObjs(zks, cnxn), new byte[0]));
    authInfo = cnxn.getAuthInfo();
    Assert.assertEquals(1, authInfo.size());
    Assert.assertEquals("super", authInfo.get(0).getScheme());
    Assert.assertEquals("SuperUser", authInfo.get(0).getId());
    System.clearProperty(X509AuthenticationConfig.DEDICATED_DOMAIN);

    // Cross domain components
    provider = createProvider(crossDomainCert);
    cnxn = new MockServerCnxn();
    cnxn.clientChain = new X509Certificate[]{crossDomainCert};
    Assert.assertEquals(KeeperException.Code.OK, provider
        .handleAuthentication(new ServerAuthenticationProvider.ServerObjs(zks, cnxn), new byte[0]));
    authInfo = cnxn.getAuthInfo();
    Assert.assertEquals(1, authInfo.size());
    Assert.assertEquals("super", authInfo.get(0).getScheme());
    Assert.assertEquals("CrossDomain", authInfo.get(0).getId());
  }

  @Test
  public void testStoreAuthedClientId() {
    System.setProperty(X509AuthenticationConfig.STORE_AUTHED_CLIENT_ID, "true");

    // Single domain user
    X509ZNodeGroupAclProvider provider = createProvider(domainXCert);
    MockServerCnxn cnxn = new MockServerCnxn();
    cnxn.clientChain = new X509Certificate[]{domainXCert};
    Assert.assertEquals(KeeperException.Code.OK, provider
        .handleAuthentication(new ServerAuthenticationProvider.ServerObjs(zks, cnxn), new byte[0]));
    List<Id> authInfo = cnxn.getAuthInfo();
    Assert.assertEquals(2, authInfo.size());
    Assert.assertEquals(SCHEME, authInfo.get(0).getScheme());
    Assert.assertEquals("DomainX", authInfo.get(0).getId());
    Assert.assertEquals(SCHEME, authInfo.get(1).getScheme());
    Assert.assertEquals("DomainXUser", authInfo.get(1).getId());

    // Cross domain component - should be same no matter this feature is on or not
    provider = createProvider(crossDomainCert);
    cnxn = new MockServerCnxn();
    cnxn.clientChain = new X509Certificate[]{crossDomainCert};
    Assert.assertEquals(KeeperException.Code.OK, provider
        .handleAuthentication(new ServerAuthenticationProvider.ServerObjs(zks, cnxn), new byte[0]));
    authInfo = cnxn.getAuthInfo();
    Assert.assertEquals(1, authInfo.size());
    Assert.assertEquals("super", authInfo.get(0).getScheme());
    Assert.assertEquals("CrossDomain", authInfo.get(0).getId());

    // Super user - should be same no matter this feature is on or not
    provider = createProvider(superCert);
    cnxn = new MockServerCnxn();
    cnxn.clientChain = new X509Certificate[]{superCert};
    Assert.assertEquals(KeeperException.Code.OK, provider
        .handleAuthentication(new ServerAuthenticationProvider.ServerObjs(zks, cnxn), new byte[0]));
    authInfo = cnxn.getAuthInfo();
    Assert.assertEquals(1, authInfo.size());
    Assert.assertEquals("super", authInfo.get(0).getScheme());
    Assert.assertEquals("SuperUser", authInfo.get(0).getId());

    System.clearProperty(X509AuthenticationConfig.STORE_AUTHED_CLIENT_ID);
  }

  private X509ZNodeGroupAclProvider createProvider(X509Certificate trustedCert) {
    return new X509ZNodeGroupAclProvider(new X509AuthTest.TestTrustManager(trustedCert),
        new X509AuthTest.TestKeyManager());
  }

  private MockServerCnxn authenticateSpiffe(String path) throws Exception {
    SpiffeAuthTestUtil.registerBouncyCastle();
    X509AuthenticationConfig.reset();
    X509Certificate cert = SpiffeAuthTestUtil.buildClientCertWithUriSans("spiffe://example.org" + path);
    X509ZNodeGroupAclProvider provider = new X509ZNodeGroupAclProvider(
        new SpiffeAuthTestUtil.AcceptAllTrustManager(), new SpiffeAuthTestUtil.NoopKeyManager());
    MockServerCnxn cnxn = new MockServerCnxn();
    cnxn.clientChain = new X509Certificate[]{cert};
    Assert.assertEquals(KeeperException.Code.OK,
        provider.handleAuthentication(new ServerAuthenticationProvider.ServerObjs(zks, cnxn), null));
    return cnxn;
  }

  /**
   * Special ServerCnxnFactory which Exposes the client list for testing auto-refresh AuthInfo.
   */
  class TestNIOServerCnxnFactory extends NIOServerCnxnFactory {
    Set<ServerCnxn> getClients() {
      return cnxns;
    }
  }

  private static class ClosableMockServerCnxn extends MockServerCnxn {
    private boolean isClosed = false;

    @Override
    public void close(DisconnectReason reason) {
      isClosed = true;
    }

    public boolean isClosed() {
      return isClosed;
    }
  }
}
