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

package org.apache.zookeeper.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import com.google.common.annotations.VisibleForTesting;
import java.math.BigInteger;
import java.net.Socket;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.common.SpiffeAuthTestUtil;
import org.apache.zookeeper.server.MockServerCnxn;
import org.apache.zookeeper.server.auth.X509AuthenticationConfig;
import org.apache.zookeeper.server.auth.X509AuthenticationProvider;
import org.junit.Before;
import org.junit.Test;

public class X509AuthTest extends ZKTestCase {

    private static TestCertificate clientCert;
    private static TestCertificate superCert;
    private static TestCertificate unknownCert;

    @Before
    public void setUp() {
        System.setProperty("zookeeper.X509AuthenticationProvider.superUser", "CN=SUPER");
        System.setProperty("zookeeper.ssl.keyManager", "org.apache.zookeeper.test.X509AuthTest.TestKeyManager");
        System.setProperty("zookeeper.ssl.trustManager", "org.apache.zookeeper.test.X509AuthTest.TestTrustManager");

        clientCert = new TestCertificate("CLIENT");
        superCert = new TestCertificate("SUPER");
        unknownCert = new TestCertificate("UNKNOWN");
    }

    @Test
    public void testTrustedAuth() {
        X509AuthenticationProvider provider = createProvider(clientCert);
        MockServerCnxn cnxn = new MockServerCnxn();
        cnxn.clientChain = new X509Certificate[]{clientCert};
        assertEquals(KeeperException.Code.OK, provider.handleAuthentication(cnxn, null));
    }

    @Test
    public void testSuperAuth() {
        X509AuthenticationProvider provider = createProvider(superCert);
        MockServerCnxn cnxn = new MockServerCnxn();
        cnxn.clientChain = new X509Certificate[]{superCert};
        assertEquals(KeeperException.Code.OK, provider.handleAuthentication(cnxn, null));
        assertEquals("super", cnxn.getAuthInfo().get(0).getScheme());
    }

    @Test
    public void testUntrustedAuth() {
        X509AuthenticationProvider provider = createProvider(clientCert);
        MockServerCnxn cnxn = new MockServerCnxn();
        cnxn.clientChain = new X509Certificate[]{unknownCert};
        assertEquals(KeeperException.Code.AUTHFAILED, provider.handleAuthentication(cnxn, null));
    }

    @Test
    public void testSANBasedAuth() {
        String clientCertIdType = "SAN";
        String clientCertIdSANMatchType = "6";
        // The following clientCertIdSANMatchRegex matches the entire SAN String
        String clientCertIdSANMatchRegex = ".*";
        // TEST_SAN_STR = "a:b:c(d;e;f)" in the test. The following clientCertIdSANExtractRegex
        // extracts the first element in the parentheses excluding "a:b:c(" and trailing ";*"
        String clientCertIdSANExtractRegex = "^a:b:c\\((.+);.+;.+\\)$";
        // The following clientCertIdSANExtractMatcherGroupIndex specifies the first index in the
        // Matcher group, which is "d"
        String clientCertIdSANExtractMatcherGroupIndex = "1";
        String expectedClientIdFromSANExtraction = "d";

    // Set JVM properties to enable SAN-based client id extraction
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_TYPE, clientCertIdType);
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_TYPE, clientCertIdSANMatchType);
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_REGEX, clientCertIdSANMatchRegex);
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_REGEX, clientCertIdSANExtractRegex);
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_MATCHER_GROUP_INDEX,
        clientCertIdSANExtractMatcherGroupIndex);

        X509AuthenticationProvider provider = createProvider(clientCert);
        MockServerCnxn cnxn = new MockServerCnxn();
        cnxn.clientChain = new X509Certificate[]{clientCert};
        assertEquals(KeeperException.Code.OK, provider.handleAuthentication(cnxn, null));
        assertEquals(expectedClientIdFromSANExtraction, cnxn.getAuthInfo().get(0).getId());

    // Remove JVM properties so they don't interfere with other tests
    System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_TYPE);
    System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_TYPE);
    System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_REGEX);
    System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_REGEX);
    System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_MATCHER_GROUP_INDEX);
    X509AuthenticationConfig.reset();
  }

  // SPIFFE test fixtures
  private static final String SPIFFE_V1_URI = "spiffe://prod.lipki/v1/wl/espresso-router";
  private static final String SPIFFE_V2_URI = "spiffe://prod.lipki/v2/application/espresso-router/espresso-router";

  @Test
  public void testSpiffeV1ExtractedWithoutAnyClientCertIdTypeConfigured() {
    // Core "not a feature flag" guarantee: SPIFFE detection runs regardless of
    // clientCertIdType. No system properties are set at all here (not even
    // clientCertIdType=SAN) — the cert's spiffe:// URI SAN must still be recognized and
    // extracted without any operator configuration.
    assertNull("Test must start with no clientCertIdType configured",
        System.getProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_TYPE));
    try {
      TestCertificate spiffeCert = new TestCertificate("CLIENT", SPIFFE_V1_URI);
      X509AuthenticationProvider provider = createProvider(spiffeCert);
      MockServerCnxn cnxn = new MockServerCnxn();
      cnxn.clientChain = new X509Certificate[]{spiffeCert};

      assertEquals(KeeperException.Code.OK, provider.handleAuthentication(cnxn, null));
      assertEquals("espresso-router", cnxn.getAuthInfo().get(0).getId());
    } finally {
      X509AuthenticationConfig.reset();
    }
  }

  @Test
  public void testSpiffeV2ExtractedWithoutAnyClientCertIdTypeConfigured() {
    // Same guarantee as above, for the v2 (full ILM UID) form.
    assertNull("Test must start with no clientCertIdType configured",
        System.getProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_TYPE));
    try {
      TestCertificate spiffeCert = new TestCertificate("CLIENT", SPIFFE_V2_URI);
      X509AuthenticationProvider provider = createProvider(spiffeCert);
      MockServerCnxn cnxn = new MockServerCnxn();
      cnxn.clientChain = new X509Certificate[]{spiffeCert};

      assertEquals(KeeperException.Code.OK, provider.handleAuthentication(cnxn, null));
      assertEquals("application/espresso-router/espresso-router",
          cnxn.getAuthInfo().get(0).getId());
    } finally {
      X509AuthenticationConfig.reset();
    }
  }

  @Test
  public void testSpiffeV2UserIdentityRejectedWithoutAnyClientCertIdTypeConfigured() {
    // Rejection of user identities must also hold with zero configuration — a human's SPIFFE
    // cert must never be promoted to a service principal, feature flag or not.
    String spiffeUserUri = "spiffe://prod.lipki/v2/user/alice";
    assertNull("Test must start with no clientCertIdType configured",
        System.getProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_TYPE));
    try {
      TestCertificate userCert = new TestCertificate("CLIENT", spiffeUserUri);
      X509AuthenticationProvider provider = createProvider(userCert);
      MockServerCnxn cnxn = new MockServerCnxn();
      cnxn.clientChain = new X509Certificate[]{userCert};

      assertEquals(KeeperException.Code.OK, provider.handleAuthentication(cnxn, null));
      assertEquals("CN=CLIENT", cnxn.getAuthInfo().get(0).getId());
    } finally {
      X509AuthenticationConfig.reset();
    }
  }

  @Test
  public void testSpiffeV1WlAuth() {
    SpiffeAuthTestUtil.setSpiffeSystemProperties();
    try {
      // SPIFFE_V1_URI = "spiffe://prod.lipki/v1/wl/espresso-router"; the "wl/" type prefix is
      // stripped, principal is just the app-name.
      TestCertificate spiffeCert = new TestCertificate("CLIENT", SPIFFE_V1_URI);
      X509AuthenticationProvider provider = createProvider(spiffeCert);
      MockServerCnxn cnxn = new MockServerCnxn();
      cnxn.clientChain = new X509Certificate[]{spiffeCert};

      assertEquals(KeeperException.Code.OK, provider.handleAuthentication(cnxn, null));
      assertEquals("espresso-router", cnxn.getAuthInfo().get(0).getId());
    } finally {
      SpiffeAuthTestUtil.clearSpiffeSystemProperties();
    }
  }

  @Test
  public void testSpiffeV1WlMultiSegmentFallsBackToDn() {
    // v1/wl app-name must be a single path segment. A multi-segment value after "wl/" (e.g.
    // "a/b") must NOT be accepted as a single app-name principal; it should fall through to
    // URN (not configured) then to Subject DN.
    String spiffeMultiSegmentUri = "spiffe://prod.lipki/v1/wl/a/b";
    SpiffeAuthTestUtil.setSpiffeSystemProperties();
    try {
      TestCertificate spiffeCert = new TestCertificate("CLIENT", spiffeMultiSegmentUri);
      X509AuthenticationProvider provider = createProvider(spiffeCert);
      MockServerCnxn cnxn = new MockServerCnxn();
      cnxn.clientChain = new X509Certificate[]{spiffeCert};

      assertEquals(KeeperException.Code.OK, provider.handleAuthentication(cnxn, null));
      assertEquals("CN=CLIENT", cnxn.getAuthInfo().get(0).getId());
    } finally {
      SpiffeAuthTestUtil.clearSpiffeSystemProperties();
    }
  }

  @Test
  public void testSpiffeV2Auth() {
    SpiffeAuthTestUtil.setSpiffeSystemProperties();
    try {
      TestCertificate spiffeCert = new TestCertificate("CLIENT", SPIFFE_V2_URI);
      X509AuthenticationProvider provider = createProvider(spiffeCert);
      MockServerCnxn cnxn = new MockServerCnxn();
      cnxn.clientChain = new X509Certificate[]{spiffeCert};

      assertEquals(KeeperException.Code.OK, provider.handleAuthentication(cnxn, null));
      assertEquals("application/espresso-router/espresso-router",
          cnxn.getAuthInfo().get(0).getId());
    } finally {
      SpiffeAuthTestUtil.clearSpiffeSystemProperties();
    }
  }

  @Test
  public void testSpiffeV2WorkloadAuth() {
    SpiffeAuthTestUtil.setSpiffeSystemProperties();
    try {
      String spiffeWorkloadUri = "spiffe://prod.lipki/v2/workload/foo-mp/bar-app/some-tag";
      TestCertificate spiffeCert = new TestCertificate("CLIENT", spiffeWorkloadUri);
      X509AuthenticationProvider provider = createProvider(spiffeCert);
      MockServerCnxn cnxn = new MockServerCnxn();
      cnxn.clientChain = new X509Certificate[]{spiffeCert};

      assertEquals(KeeperException.Code.OK, provider.handleAuthentication(cnxn, null));
      assertEquals("workload/foo-mp/bar-app/some-tag", cnxn.getAuthInfo().get(0).getId());
    } finally {
      SpiffeAuthTestUtil.clearSpiffeSystemProperties();
    }
  }

  @Test
  public void testSpiffeNotConfiguredFallsBackToUrn() {
    // Cert has a URN SAN (not a spiffe:// URI), so the always-on SPIFFE check finds no match and
    // falls through to legacy URN-based SAN extraction.
    String urnSan = "urn:li:servicePrincipal(espresso-router;ei4;i001)";
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_TYPE, "SAN");
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_TYPE, "6");
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_REGEX, "^.*urn:li:.*$");
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_REGEX,
        "^.*urn:li:([a-z]+Principal\\([^;%:]+)");
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_MATCHER_GROUP_INDEX, "1");
    // SPIFFE detection is always on (not config-gated); it simply finds no spiffe:// SAN here.

    try {
      TestCertificate urnCert = new TestCertificate("CLIENT", urnSan);
      X509AuthenticationProvider provider = createProvider(urnCert);
      MockServerCnxn cnxn = new MockServerCnxn();
      cnxn.clientChain = new X509Certificate[]{urnCert};

      assertEquals(KeeperException.Code.OK, provider.handleAuthentication(cnxn, null));
      assertEquals("servicePrincipal(espresso-router", cnxn.getAuthInfo().get(0).getId());
    } finally {
      System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_TYPE);
      System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_TYPE);
      System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_REGEX);
      System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_REGEX);
      System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_MATCHER_GROUP_INDEX);
      X509AuthenticationConfig.reset();
    }
  }

  @Test
  public void testSpiffeConfiguredButNoSpiffeSanFallsBackToUrn() {
    // Cert has a URN SAN (not SPIFFE) → the always-on SPIFFE check returns empty → falls back to URN
    String urnSan = "urn:li:servicePrincipal(espresso-router;ei4;i001)";
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_TYPE, "SAN");
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_TYPE, "6");
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_REGEX, "^.*urn:li:.*$");
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_REGEX,
        "^.*urn:li:([a-z]+Principal\\([^;%:]+)");
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_MATCHER_GROUP_INDEX, "1");

    try {
      TestCertificate urnCert = new TestCertificate("CLIENT", urnSan);
      X509AuthenticationProvider provider = createProvider(urnCert);
      MockServerCnxn cnxn = new MockServerCnxn();
      cnxn.clientChain = new X509Certificate[]{urnCert};

      assertEquals(KeeperException.Code.OK, provider.handleAuthentication(cnxn, null));
      // URN SAN doesn't match spiffe://, so falls through to URN extraction
      assertEquals("servicePrincipal(espresso-router", cnxn.getAuthInfo().get(0).getId());
    } finally {
      System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_TYPE);
      System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_TYPE);
      System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_REGEX);
      System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_REGEX);
      System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_MATCHER_GROUP_INDEX);
      X509AuthenticationConfig.reset();
    }
  }

  @Test
  public void testUrnMatchRegexTooBroadWithGrestinMetadataSanFallsBackToDn() {
    // Real Grestin-issued service certs carry TWO urn:li: URIs in the same cert:
    // servicePrincipal(...) and servicePrincipalMetadata(...). A loose match regex like
    // "^.*urn:li:.*$" matches BOTH, which findSingleMatchingSan() rejects (requires exactly one
    // match), causing a fall back to Subject DN instead of the intended service principal.
    String servicePrincipalSan = "urn:li:servicePrincipal(zk-test-client;None;i001)";
    String servicePrincipalMetadataSan = "urn:li:servicePrincipalMetadata(dev;1.0.0)";
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_TYPE, "SAN");
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_TYPE, "6");
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_REGEX, "^.*urn:li:.*$");
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_REGEX,
        "^.*urn:li:([a-z]+Principal\\([^;%:]+)");
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_MATCHER_GROUP_INDEX, "1");

    try {
      TestCertificate grestinCert = new TestCertificate("CLIENT",
          Arrays.asList(servicePrincipalSan, servicePrincipalMetadataSan));
      X509AuthenticationProvider provider = createProvider(grestinCert);
      MockServerCnxn cnxn = new MockServerCnxn();
      cnxn.clientChain = new X509Certificate[]{grestinCert};

      assertEquals(KeeperException.Code.OK, provider.handleAuthentication(cnxn, null));
      // Multiple SAN matches -> extractor throws -> falls back to Subject DN.
      assertEquals("CN=CLIENT", cnxn.getAuthInfo().get(0).getId());
    } finally {
      System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_TYPE);
      System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_TYPE);
      System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_REGEX);
      System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_REGEX);
      System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_MATCHER_GROUP_INDEX);
      X509AuthenticationConfig.reset();
    }
  }

  @Test
  public void testUrnMatchRegexAnchoredToServicePrincipalExtractsCorrectlyWithGrestinMetadataSan() {
    // Same two-SAN Grestin-style cert as above, but with a properly anchored match regex
    // (matching only servicePrincipal, not servicePrincipalMetadata). This is the
    // production-correct configuration and must yield exactly one match, extracting the
    // service principal even with SPIFFE support also configured alongside it.
    String servicePrincipalSan = "urn:li:servicePrincipal(zk-test-client;None;i001)";
    String servicePrincipalMetadataSan = "urn:li:servicePrincipalMetadata(dev;1.0.0)";
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_TYPE, "SAN");
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_TYPE, "6");
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_REGEX,
        "^.*urn:li:servicePrincipal\\(.*$");
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_REGEX,
        "^.*urn:li:([a-z]+Principal\\([^;%:]+)");
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_MATCHER_GROUP_INDEX, "1");
    // SPIFFE detection is always on; this cert has no spiffe:// SAN, so it's unaffected.

    try {
      TestCertificate grestinCert = new TestCertificate("CLIENT",
          Arrays.asList(servicePrincipalSan, servicePrincipalMetadataSan));
      X509AuthenticationProvider provider = createProvider(grestinCert);
      MockServerCnxn cnxn = new MockServerCnxn();
      cnxn.clientChain = new X509Certificate[]{grestinCert};

      assertEquals(KeeperException.Code.OK, provider.handleAuthentication(cnxn, null));
      assertEquals("servicePrincipal(zk-test-client", cnxn.getAuthInfo().get(0).getId());
    } finally {
      System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_TYPE);
      System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_TYPE);
      System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_REGEX);
      System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_REGEX);
      System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_MATCHER_GROUP_INDEX);
      X509AuthenticationConfig.reset();
    }
  }

  @Test
  public void testSpiffeV2UserIdentityRejected() {
    // SPIFFE user identity (/v2/user/<ldap>) must NOT be mapped to a service principal.
    // It should be silently rejected by the SPIFFE extractor, fall through to URN (no match),
    // then fall back to Subject DN.
    String spiffeUserUri = "spiffe://prod.lipki/v2/user/alice";
    SpiffeAuthTestUtil.setSpiffeSystemProperties();
    try {
      TestCertificate userCert = new TestCertificate("CLIENT", spiffeUserUri);
      X509AuthenticationProvider provider = createProvider(userCert);
      MockServerCnxn cnxn = new MockServerCnxn();
      cnxn.clientChain = new X509Certificate[]{userCert};

      assertEquals(KeeperException.Code.OK, provider.handleAuthentication(cnxn, null));
      assertEquals("CN=CLIENT", cnxn.getAuthInfo().get(0).getId());
    } finally {
      SpiffeAuthTestUtil.clearSpiffeSystemProperties();
    }
  }

  @Test
  public void testSpiffeV1UserIdentityRejected() {
    // v1 user-identity now falls back to DN for two reasons: user-identity rejection AND v1 rejection.
    String spiffeUserUri = "spiffe://prod.lipki/v1/user/alice";
    SpiffeAuthTestUtil.setSpiffeSystemProperties();
    try {
      TestCertificate userCert = new TestCertificate("CLIENT", spiffeUserUri);
      X509AuthenticationProvider provider = createProvider(userCert);
      MockServerCnxn cnxn = new MockServerCnxn();
      cnxn.clientChain = new X509Certificate[]{userCert};

      assertEquals(KeeperException.Code.OK, provider.handleAuthentication(cnxn, null));
      assertEquals("CN=CLIENT", cnxn.getAuthInfo().get(0).getId());
    } finally {
      SpiffeAuthTestUtil.clearSpiffeSystemProperties();
    }
  }

  @Test
  public void testSpiffeMultipleSpiffeSansFallsBackToDn() {
    // Cert with >1 SPIFFE SAN — extractor throws, caught in getClientId, falls through to URN
    // (not configured) then to Subject DN.
    SpiffeAuthTestUtil.setSpiffeSystemProperties();
    try {
      TestCertificate multiSanCert = new TestCertificate("CLIENT",
          Arrays.asList(SPIFFE_V2_URI, "spiffe://prod.lipki/v2/application/another-service/another-service"));
      X509AuthenticationProvider provider = createProvider(multiSanCert);
      MockServerCnxn cnxn = new MockServerCnxn();
      cnxn.clientChain = new X509Certificate[]{multiSanCert};

      assertEquals(KeeperException.Code.OK, provider.handleAuthentication(cnxn, null));
      assertEquals("CN=CLIENT", cnxn.getAuthInfo().get(0).getId());
    } finally {
      SpiffeAuthTestUtil.clearSpiffeSystemProperties();
    }
  }

  @Test
  public void testSpiffePrefersSpiffeOverUrnWhenBothPresent() {
    // Cert has BOTH a URN-format SAN and a SPIFFE SAN. SPIFFE must win.
    String urnSan = "urn:li:servicePrincipal(legacy-app;ei4;i001)";
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_TYPE, "SAN");
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_TYPE, "6");
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_REGEX, "^.*urn:li:.*$");
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_REGEX,
        "^.*urn:li:([a-z]+Principal\\([^;%:]+)");
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_MATCHER_GROUP_INDEX, "1");
    // SPIFFE detection is always on and is tried first, so it wins regardless of URN config.

    try {
      TestCertificate mixedCert = new TestCertificate("CLIENT", Arrays.asList(urnSan, SPIFFE_V2_URI));
      X509AuthenticationProvider provider = createProvider(mixedCert);
      MockServerCnxn cnxn = new MockServerCnxn();
      cnxn.clientChain = new X509Certificate[]{mixedCert};

      assertEquals(KeeperException.Code.OK, provider.handleAuthentication(cnxn, null));
      // SPIFFE wins — path-after-/v2/, not the URN-derived legacy-app id.
      assertEquals("application/espresso-router/espresso-router",
          cnxn.getAuthInfo().get(0).getId());
    } finally {
      System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_TYPE);
      System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_REGEX);
      System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_REGEX);
      System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_MATCHER_GROUP_INDEX);
      SpiffeAuthTestUtil.clearSpiffeSystemProperties();
    }
  }

  protected static class TestPublicKey implements PublicKey {

        private static final long serialVersionUID = 1L;
        @Override
        public String getAlgorithm() {
            return null;
        }
        @Override
        public String getFormat() {
            return null;
        }
        @Override
        public byte[] getEncoded() {
            return null;
        }

    }

    public static class TestCertificate extends X509Certificate {
        @VisibleForTesting
        static final String TEST_SAN_STR = "a:b:c(d;e;f)";
        private byte[] encoded;
        private X500Principal principal;
        private PublicKey publicKey;
        private List<String> subjectAlternativeNames;

        public TestCertificate(String name) {
          this(name, TEST_SAN_STR);
        }

        public TestCertificate(String name, String sanVal) {
          this(name, Collections.singletonList(sanVal));
        }

        public TestCertificate(String name, List<String> sanVals) {
          encoded = name.getBytes();
          principal = new X500Principal("CN=" + name);
          publicKey = new TestPublicKey();
          subjectAlternativeNames = sanVals;
        }
          @Override
        public boolean hasUnsupportedCriticalExtension() {
            return false;
        }
        @Override
        public Set<String> getCriticalExtensionOIDs() {
            return null;
        }
        @Override
        public Set<String> getNonCriticalExtensionOIDs() {
            return null;
        }
        @Override
        public byte[] getExtensionValue(String oid) {
            return null;
        }
        @Override
        public void checkValidity() throws CertificateExpiredException, CertificateNotYetValidException {
        }
        @Override
        public void checkValidity(Date date) throws CertificateExpiredException, CertificateNotYetValidException {
        }
        @Override
        public int getVersion() {
            return 0;
        }
        @Override
        public BigInteger getSerialNumber() {
            return null;
        }
        @Override
        public Principal getIssuerDN() {
            return null;
        }
        @Override
        public Principal getSubjectDN() {
            return null;
        }
        @Override
        public Date getNotBefore() {
            return null;
        }
        @Override
        public Date getNotAfter() {
            return null;
        }
        @Override
        public byte[] getTBSCertificate() throws CertificateEncodingException {
            return null;
        }
        @Override
        public byte[] getSignature() {
            return null;
        }
        @Override
        public String getSigAlgName() {
            return null;
        }
        @Override
        public String getSigAlgOID() {
            return null;
        }
        @Override
        public byte[] getSigAlgParams() {
            return null;
        }
        @Override
        public boolean[] getIssuerUniqueID() {
            return null;
        }
        @Override
        public boolean[] getSubjectUniqueID() {
            return null;
        }
        @Override
        public boolean[] getKeyUsage() {
            return null;
        }
        @Override
        public int getBasicConstraints() {
            return 0;
        }
        @Override
        public byte[] getEncoded() throws CertificateEncodingException {
            return encoded;
        }
        @Override
        public void verify(PublicKey key) throws CertificateException, NoSuchAlgorithmException, InvalidKeyException, NoSuchProviderException, SignatureException {
        }
        @Override
        public void verify(PublicKey key, String sigProvider) throws CertificateException, NoSuchAlgorithmException, InvalidKeyException, NoSuchProviderException, SignatureException {
        }
        @Override
        public String toString() {
            return null;
        }
        @Override
        public PublicKey getPublicKey() {
            return publicKey;
        }
        @Override
        public X500Principal getSubjectX500Principal() {
            return principal;
        }
        @Override
        public Collection<List<?>> getSubjectAlternativeNames() {
            List<List<?>> result = new ArrayList<>();
            for (String san : subjectAlternativeNames) {
                List<Object> pair = new ArrayList<>();
                pair.add(6);
                pair.add(san);
                result.add(pair);
            }
            return result;
        }
    }

    public static class TestKeyManager implements X509KeyManager {

        @Override
        public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket socket) {
            return null;
        }
        @Override
        public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
            return null;
        }
        @Override
        public X509Certificate[] getCertificateChain(String alias) {
            return null;
        }
        @Override
        public String[] getClientAliases(String keyType, Principal[] issuers) {
            return null;
        }
        @Override
        public PrivateKey getPrivateKey(String alias) {
            return null;
        }
        @Override
        public String[] getServerAliases(String keyType, Principal[] issuers) {
            return null;
        }

    }

    public static class TestTrustManager implements X509TrustManager {

        X509Certificate cert;
        public TestTrustManager(X509Certificate testCert) {
            cert = testCert;
        }
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            if (!Arrays.equals(cert.getEncoded(), chain[0].getEncoded())) {
                throw new CertificateException("Client cert not trusted");
            }
        }
        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            if (!Arrays.equals(cert.getEncoded(), chain[0].getEncoded())) {
                throw new CertificateException("Server cert not trusted");
            }
        }
        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return null;
        }

    }

    protected X509AuthenticationProvider createProvider(X509Certificate trustedCert) {
        return new X509AuthenticationProvider(new TestTrustManager(trustedCert), new TestKeyManager());
    }

}
