package org.apache.zookeeper.server.auth.znode.groupacl;

import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.common.ClientX509Util;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.MockServerCnxn;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.auth.ServerAuthenticationProvider;
import org.apache.zookeeper.server.auth.X509AuthenticationProvider;
import org.apache.zookeeper.test.ClientBase;
import org.apache.zookeeper.test.X509AuthTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class X509ZNodeGroupAclProviderTest extends ZKTestCase {
  private static final Logger LOG = LoggerFactory.getLogger(X509ZNodeGroupAclProviderTest.class);
  private static final String HOSTPORT = "127.0.0.1:" + PortAssignment.unique();
  private static X509AuthTest.TestCertificate domainXCert;
  private static X509AuthTest.TestCertificate superCert;
  private static X509AuthTest.TestCertificate unknownCert;
  private static final String CLIENT_CERT_ID_TYPE = "SAN";
  private static final String CLIENT_CERT_ID_SAN_MATCH_TYPE = "6";
  private static ZooKeeperServer zks;
  private ServerCnxnFactory serverCnxnFactory;
  private ZooKeeper admin;
  //  private ServerCnxn cnxn;
  private static final String AUTH_PROVIDER_PROPERTY_NAME = "zookeeper.authProvider";
  private static final String CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH = "/_CLIENT_URI_DOMAIN_MAPPING";
  private static final String[] MAPPING_PATHS = {CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH,
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/CrossDomain",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/CrossDomain/CrossDomainApp",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/DomainX",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/DomainX/DomainXUser",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/DomainY",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/DomainY/DomainYUser"};
  private static final Map<String, String> NODE_PATHS_AND_ACLS;

  static {
    NODE_PATHS_AND_ACLS = new HashMap<>();
    NODE_PATHS_AND_ACLS.put("/node/domainX", "DomainX");
    NODE_PATHS_AND_ACLS.put("/node/domainY", "DomainY");
    NODE_PATHS_AND_ACLS.put("/node/public", "");
    NODE_PATHS_AND_ACLS.put("/node/open_read_access", "DomainX");
  }

  @Before
  public void setUp() throws Exception {
    ClientX509Util util = new ClientX509Util();
    System.setProperty(X509AuthenticationProvider.ZOOKEEPER_X509AUTHENTICATIONPROVIDER_SUPERUSER,
        "CN=SUPER");
    System.setProperty("zookeeper.ssl.keyManager",
        "org.apache.zookeeper.test.X509AuthTest.TestKeyManager");
    System.setProperty("zookeeper.ssl.trustManager",
        "org.apache.zookeeper.test.X509AuthTest.TestTrustManager");
    System.setProperty(util.sslClientCertIdType, CLIENT_CERT_ID_TYPE);
    System.setProperty(util.sslClientCertIdSanMatchType, CLIENT_CERT_ID_SAN_MATCH_TYPE);
    System.setProperty(AUTH_PROVIDER_PROPERTY_NAME, X509ZNodeGroupAclProvider.class.getName());
    System.setProperty(
        ZNodeGroupAclUtil.ZNODE_GROUP_ACL_CONFIG_PREFIX + "clientUriDomainMappingRootPath",
        CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH);
    LOG.info("Starting Zk...");
    zks = new ZooKeeperServer(testBaseDir, testBaseDir, 3000);
    final int PORT = Integer.parseInt(HOSTPORT.split(":")[1]);
    serverCnxnFactory = ServerCnxnFactory.createFactory(PORT, -1);
    serverCnxnFactory.startup(zks);
    LOG.info("Waiting for server startup");
    Assert.assertTrue("waiting for server being up ", ClientBase.waitForServerUp(HOSTPORT, 300000));
    admin = ClientBase.createZKClient(HOSTPORT);
//      cnxn = serverCnxnFactory.getConnections().iterator().next();
    domainXCert = new X509AuthTest.TestCertificate("CLIENT", "DomainXUser");
    superCert = new X509AuthTest.TestCertificate("SUPER", "CrossDomainApp");
    unknownCert = new X509AuthTest.TestCertificate("UNKNOWN", "UnknownUser");

    for (String path : MAPPING_PATHS) {
      // Create ACL metadata
      admin.create(path, null, OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    // Create node with ACL to be accessed
    for (Map.Entry<String, String> znode : NODE_PATHS_AND_ACLS.entrySet()) {
      List<ACL> acl;
      if (znode.getValue().isEmpty()) {
        acl = OPEN_ACL_UNSAFE;
      } else {
        acl = new ArrayList<>();
        acl.add(new ACL(ZooDefs.Perms.ALL, new Id("x509", znode.getValue())));
      }
      admin.create(znode.getKey(), null, acl, CreateMode.PERSISTENT);

    }
  }

  @After
  public void cleanUp() throws InterruptedException, KeeperException {
    ZKUtil.deleteRecursive(admin, CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH);
    zks.shutdown();
    admin.close();
    serverCnxnFactory.shutdown();
    ClientX509Util util = new ClientX509Util();
    System.clearProperty(X509AuthenticationProvider.ZOOKEEPER_X509AUTHENTICATIONPROVIDER_SUPERUSER);
    System.clearProperty(util.sslClientCertIdType);
    System.clearProperty(util.sslClientCertIdSanMatchType);
    System.clearProperty(AUTH_PROVIDER_PROPERTY_NAME);
    System.clearProperty(
        ZNodeGroupAclUtil.ZNODE_GROUP_ACL_CONFIG_PREFIX + "clientUriDomainMappingRootPath");
  }

  @Test(expected = KeeperException.AuthFailedException.class)
  public void testUntrustedClient() {
    X509ZNodeGroupAclProvider provider = createProvider(unknownCert);
    MockServerCnxn cnxn = new MockServerCnxn();
    cnxn.clientChain = new X509Certificate[]{unknownCert};
    try {
      provider.handleAuthentication(new ServerAuthenticationProvider.ServerObjs(zks, cnxn),
          new byte[0]);
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Failed to trust certificate"));
      throw e;
    }
  }

  @Test
  public void testAuthorizedClient() {
    X509ZNodeGroupAclProvider provider = createProvider(domainXCert);
//    cnxn.setClientCertificateChain(new Certificate[]{domainXCert});
  }

  @Test
  public void testUnauthorizedClient() {
//    cnxn.setClientCertificateChain(new Certificate[]{domainXCert});
  }

  @Test
  public void testSuperUser() {
//    cnxn.setClientCertificateChain(new Certificate[]{superCert});
  }

  @Test
  public void testOpenReadAccess() {
//    cnxn.setClientCertificateChain(new Certificate[]{unknownCert});
  }

  private X509ZNodeGroupAclProvider createProvider(X509Certificate trustedCert) {
    return new X509ZNodeGroupAclProvider(new X509AuthTest.TestTrustManager(trustedCert),
        new X509AuthTest.TestKeyManager());
  }
}

