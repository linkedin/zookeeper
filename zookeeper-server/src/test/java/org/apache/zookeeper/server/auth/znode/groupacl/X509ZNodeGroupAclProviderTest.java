package org.apache.zookeeper.server.auth.znode.groupacl;

import java.security.cert.X509Certificate;
import java.util.List;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.MockServerCnxn;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.auth.ServerAuthenticationProvider;
import org.apache.zookeeper.server.auth.X509AuthenticationUtil;
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
  private static X509AuthTest.TestCertificate unknownCert;
  private static final String CLIENT_CERT_ID_TYPE = "SAN";
  private static final String CLIENT_CERT_ID_SAN_MATCH_TYPE = "6";
  private static final String SCHEME = "x509";
  private static ZooKeeperServer zks;
  private ServerCnxnFactory serverCnxnFactory;
  private ZooKeeper admin;
  private static final String AUTH_PROVIDER_PROPERTY_NAME = "zookeeper.authProvider.x509";
  private static final String CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH = "/_CLIENT_URI_DOMAIN_MAPPING";
  private static final String[] MAPPING_PATHS = {CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH,
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/CrossDomain",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/CrossDomain/SuperUser",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/DomainX",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/DomainX/DomainXUser",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/DomainY",
      CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH + "/DomainY/DomainYUser"};

  @Before
  public void setUp() throws Exception {
    System.setProperty(X509ZNodeGroupAclProvider.ZOOKEEPER_ZNODEGROUPACL_SUPERUSER, "SuperUser");
    System.setProperty("zookeeper.ssl.keyManager",
        "org.apache.zookeeper.test.X509AuthTest.TestKeyManager");
    System.setProperty("zookeeper.ssl.trustManager",
        "org.apache.zookeeper.test.X509AuthTest.TestTrustManager");
    System.setProperty(X509AuthenticationUtil.SSL_X509_CLIENT_CERT_ID_TYPE, CLIENT_CERT_ID_TYPE);
    System.setProperty(X509AuthenticationUtil.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_TYPE,
        CLIENT_CERT_ID_SAN_MATCH_TYPE);
    System.setProperty(AUTH_PROVIDER_PROPERTY_NAME, X509ZNodeGroupAclProvider.class.getCanonicalName());
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
    try {
      ZKUtil.deleteRecursive(admin, CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH);
    } catch (Exception ignored) {
    }

    // Create test client certificates
    domainXCert = new X509AuthTest.TestCertificate("CLIENT", "DomainXUser");
    superCert = new X509AuthTest.TestCertificate("SUPER", "SuperUser");
    unknownCert = new X509AuthTest.TestCertificate("UNKNOWN", "UnknownUser");

    // Create Client URI - domain mapping znodes
    for (String path : MAPPING_PATHS) {
      // Create ACL metadata
      admin.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
  }

  @After
  public void cleanUp() throws InterruptedException, KeeperException {
    ZKUtil.deleteRecursive(admin, CLIENT_URI_DOMAIN_MAPPING_ROOT_PATH);
    zks.shutdown();
    admin.close();
    serverCnxnFactory.shutdown();
    System.clearProperty(X509ZNodeGroupAclProvider.ZOOKEEPER_ZNODEGROUPACL_SUPERUSER);
    System.clearProperty(X509AuthenticationUtil.SSL_X509_CLIENT_CERT_ID_TYPE);
    System.clearProperty(X509AuthenticationUtil.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_TYPE);
    System.clearProperty(AUTH_PROVIDER_PROPERTY_NAME);
    System.clearProperty(
        ZNodeGroupAclUtil.ZNODE_GROUP_ACL_CONFIG_PREFIX + "clientUriDomainMappingRootPath");
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
    X509ZNodeGroupAclProvider provider = createProvider(superCert);
    MockServerCnxn cnxn = new MockServerCnxn();
    cnxn.clientChain = new X509Certificate[]{superCert};
    Assert.assertEquals(KeeperException.Code.OK, provider
        .handleAuthentication(new ServerAuthenticationProvider.ServerObjs(zks, cnxn), new byte[0]));
    List<Id> authInfo = cnxn.getAuthInfo();
    Assert.assertEquals(1, authInfo.size());
    Assert.assertEquals("super", authInfo.get(0).getScheme());
    Assert.assertEquals("SuperUser", authInfo.get(0).getId());
  }

  private X509ZNodeGroupAclProvider createProvider(X509Certificate trustedCert) {
    return new X509ZNodeGroupAclProvider(new X509AuthTest.TestTrustManager(trustedCert),
        new X509AuthTest.TestKeyManager());
  }
}

