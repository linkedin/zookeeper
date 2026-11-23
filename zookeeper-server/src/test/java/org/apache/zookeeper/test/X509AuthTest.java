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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.MockServerCnxn;
import org.apache.zookeeper.server.auth.ServerAuthenticationProvider;
import org.apache.zookeeper.server.auth.X509AuthenticationConfig;
import org.apache.zookeeper.server.auth.X509AuthenticationProvider;
import org.apache.zookeeper.server.auth.X509AuthenticationUtil;
import org.apache.zookeeper.server.auth.X509AuthenticationUtil.CertificateType;
import org.apache.zookeeper.server.auth.X509AuthenticationUtil.ClientIdentity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;


public class X509AuthTest extends ZKTestCase {

    private static final String SPIFFE_KAFKA_SAN = "spiffe://example.org/v2/application/example-mp/kafka";

    private static TestCertificate clientCert;
    private static TestCertificate superCert;
    private static TestCertificate unknownCert;

    @BeforeEach
    public void setUp() {
        X509AuthenticationConfig.reset();
        System.setProperty("zookeeper.X509AuthenticationProvider.superUser", "CN=SUPER");
        System.setProperty("zookeeper.ssl.keyManager", "org.apache.zookeeper.test.X509AuthTest.TestKeyManager");
        System.setProperty("zookeeper.ssl.trustManager", "org.apache.zookeeper.test.X509AuthTest.TestTrustManager");

        clientCert = new TestCertificate("CLIENT");
        superCert = new TestCertificate("SUPER");
        unknownCert = new TestCertificate("UNKNOWN");
    }

    @AfterEach
    public void tearDown() {
        System.clearProperty("zookeeper.X509AuthenticationProvider.superUser");
        System.clearProperty("zookeeper.ssl.keyManager");
        System.clearProperty("zookeeper.ssl.trustManager");
        System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_TYPE);
        System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_TYPE);
        System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_REGEX);
        System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_REGEX);
        System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_MATCHER_GROUP_INDEX);
        X509AuthenticationConfig.reset();
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

    @ParameterizedTest
    @ValueSource(strings = {"wrong-type", "no-match", "multiple-matches", "bad-group", "bad-extract"})
    public void testInvalidSanSelectionFallsBackToSubject(String scenario) {
        X509AuthenticationConfig config = X509AuthenticationConfig.getInstance();
        config.setClientCertIdType("SAN");
        config.setClientCertIdSanMatchType("6");
        TestCertificate certificate = clientCert;
        if ("wrong-type".equals(scenario)) {
            config.setClientCertIdSanMatchType("9");
        } else if ("no-match".equals(scenario)) {
            config.setClientCertIdSanMatchRegex("^no-match$");
        } else if ("bad-group".equals(scenario)) {
            config.setClientCertIdSanExtractMatcherGroupIndex("3");
        } else if ("bad-extract".equals(scenario)) {
            config.setClientCertIdSanExtractRegex("^no-match$");
        } else {
            certificate = new TestCertificate("CLIENT") {
                @Override
                public Collection<List<?>> getSubjectAlternativeNames() {
                    return Arrays.asList(Arrays.asList(6, "urn:example:one"), Arrays.asList(6, "urn:example:two"));
                }
            };
        }
        assertEquals("CN=CLIENT", X509AuthenticationUtil.getClientId(certificate).getId());
    }

    @Test
    public void testUrnMatchRegexTooBroadWithMetadataSanFallsBackToDn() {
        // Some service certs carry two urn:li: URIs: servicePrincipal(...) and
        // servicePrincipalMetadata(...). A loose match regex matches both, which is rejected
        // (exactly one match is required), causing a fall back to Subject DN.
        String servicePrincipalSan = "urn:li:servicePrincipal(zk-test-client;None;i001)";
        String servicePrincipalMetadataSan = "urn:li:servicePrincipalMetadata(dev;1.0.0)";
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_TYPE, "SAN");
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_TYPE, "6");
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_REGEX, "^.*urn:li:.*$");
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_REGEX,
            "^.*urn:li:([a-z]+Principal\\([^;%:]+)");
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_MATCHER_GROUP_INDEX, "1");

        TestCertificate serviceCert = new TestCertificate("CLIENT",
            Arrays.asList(servicePrincipalSan, servicePrincipalMetadataSan));
        X509AuthenticationProvider provider = createProvider(serviceCert);
        MockServerCnxn cnxn = new MockServerCnxn();
        cnxn.clientChain = new X509Certificate[]{serviceCert};

        assertEquals(KeeperException.Code.OK, provider.handleAuthentication(cnxn, null));
        assertEquals("CN=CLIENT", cnxn.getAuthInfo().get(0).getId());
        assertEquals(CertificateType.SUBJECT_DN, cnxn.getX509ClientIdentity().getCertificateType());
    }

    @Test
    public void testUrnMatchRegexAnchoredToServicePrincipalExtractsCorrectlyWithMetadataSan() {
        String servicePrincipalSan = "urn:li:servicePrincipal(zk-test-client;None;i001)";
        String servicePrincipalMetadataSan = "urn:li:servicePrincipalMetadata(dev;1.0.0)";
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_TYPE, "SAN");
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_TYPE, "6");
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_REGEX,
            "^.*urn:li:servicePrincipal\\(.*$");
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_REGEX,
            "^.*urn:li:([a-z]+Principal\\([^;%:]+)");
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_MATCHER_GROUP_INDEX, "1");

        TestCertificate serviceCert = new TestCertificate("CLIENT",
            Arrays.asList(servicePrincipalSan, servicePrincipalMetadataSan));
        X509AuthenticationProvider provider = createProvider(serviceCert);
        MockServerCnxn cnxn = new MockServerCnxn();
        cnxn.clientChain = new X509Certificate[]{serviceCert};

        assertEquals(KeeperException.Code.OK, provider.handleAuthentication(cnxn, null));
        assertEquals("servicePrincipal(zk-test-client", cnxn.getAuthInfo().get(0).getId());
    }

    @Test
    public void testUrnWithoutSanConfigurationKeepsSubjectDn() {
        String servicePrincipalSan = "urn:li:servicePrincipal(kafka;region1;instance1)";
        TestCertificate serviceCert = new TestCertificate("CLIENT", servicePrincipalSan);
        X509AuthenticationProvider provider = createProvider(serviceCert);
        MockServerCnxn cnxn = new MockServerCnxn();
        cnxn.clientChain = new X509Certificate[]{serviceCert};

        assertEquals(KeeperException.Code.OK, provider.handleAuthentication(cnxn, null));
        assertEquals("CN=CLIENT", cnxn.getAuthInfo().get(0).getId());
        ClientIdentity identity = X509AuthenticationUtil.getClientId(serviceCert);
        assertEquals(CertificateType.SUBJECT_DN, identity.getCertificateType());
        assertEquals("CN=CLIENT", identity.getId());
    }

    @Test
    public void testUrnWithMetadataWithoutSanConfigurationKeepsSubjectDn() {
        String servicePrincipalSan = "urn:li:servicePrincipal(kafka;region1;instance1)";
        String servicePrincipalMetadataSan = "urn:li:servicePrincipalMetadata(dev;1.0.0)";
        TestCertificate serviceCert = new TestCertificate("CLIENT",
            Arrays.asList(servicePrincipalSan, servicePrincipalMetadataSan));
        X509AuthenticationProvider provider = createProvider(serviceCert);
        MockServerCnxn cnxn = new MockServerCnxn();
        cnxn.clientChain = new X509Certificate[]{serviceCert};

        assertEquals(KeeperException.Code.OK, provider.handleAuthentication(cnxn, null));
        assertEquals("CN=CLIENT", cnxn.getAuthInfo().get(0).getId());
        assertEquals(CertificateType.SUBJECT_DN,
            X509AuthenticationUtil.getClientId(serviceCert).getCertificateType());
    }

    @Test
    public void testClientIdentityPreservesConfiguredSanExtraction() {
        String servicePrincipalSan = "urn:li:servicePrincipal(zk-test-client;region1;instance1)";
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_TYPE, "SAN");
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_TYPE, "6");
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_REGEX,
            "^.*urn:li:servicePrincipal\\(.*$");
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_REGEX,
            "^.*urn:li:([a-z]+Principal\\([^;%:]+)");
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_MATCHER_GROUP_INDEX, "1");

        TestCertificate serviceCert = new TestCertificate("CLIENT", servicePrincipalSan);
        X509AuthenticationProvider provider = createProvider(serviceCert);
        MockServerCnxn cnxn = new MockServerCnxn();
        cnxn.clientChain = new X509Certificate[]{serviceCert};

        assertEquals(KeeperException.Code.OK, provider.handleAuthentication(cnxn, null));
        assertEquals("servicePrincipal(zk-test-client", cnxn.getAuthInfo().get(0).getId());
        ClientIdentity identity = X509AuthenticationUtil.getClientId(serviceCert);
        assertEquals(CertificateType.LEGACY_SAN, identity.getCertificateType());
        assertEquals("servicePrincipal(zk-test-client", identity.getId());
        assertEquals("servicePrincipal(zk-test-client", X509AuthenticationUtil.getClientId(serviceCert).getId());
    }

    @Test
    public void testClientIdentityPreservesSubjectDn() {
        TestCertificate certWithoutUrnSan = new TestCertificate("CLIENT");
        X509AuthenticationProvider provider = createProvider(certWithoutUrnSan);
        MockServerCnxn cnxn = new MockServerCnxn();
        cnxn.clientChain = new X509Certificate[]{certWithoutUrnSan};

        assertEquals(KeeperException.Code.OK, provider.handleAuthentication(cnxn, null));
        assertEquals("CN=CLIENT", cnxn.getAuthInfo().get(0).getId());
        assertEquals(CertificateType.SUBJECT_DN,
            X509AuthenticationUtil.getClientId(certWithoutUrnSan).getCertificateType());
    }

    @Test
    public void testUnmatchedOptionalSanCaptureFallsBackToSubjectDn() {
        // The extract regex matches, but its selected optional group does not participate.
        X509AuthenticationConfig config = X509AuthenticationConfig.getInstance();
        config.setClientCertIdType("SAN");
        config.setClientCertIdSanMatchType("6");
        config.setClientCertIdSanMatchRegex(".*");
        config.setClientCertIdSanExtractRegex("^a:b:c\\((missing)?.*$");
        config.setClientCertIdSanExtractMatcherGroupIndex("1");

        ClientIdentity identity = X509AuthenticationUtil.getClientId(clientCert);
        assertEquals(CertificateType.SUBJECT_DN, identity.getCertificateType());
        assertEquals("CN=CLIENT", identity.getId());
        assertEquals("CN=CLIENT", X509AuthenticationUtil.getClientId(clientCert).getId());

        X509AuthenticationProvider provider = createProvider(clientCert);
        MockServerCnxn cnxn = new MockServerCnxn();
        cnxn.clientChain = new X509Certificate[]{clientCert};
        assertEquals(KeeperException.Code.OK, provider.handleAuthentication(cnxn, null));
        assertEquals(1, cnxn.getAuthInfo().size());
        assertEquals("x509", cnxn.getAuthInfo().get(0).getScheme());
        assertEquals("CN=CLIENT", cnxn.getAuthInfo().get(0).getId());
    }

    @Test
    public void testMissingSanFallsBackToSubjectDnWithoutSpiffeIdentity() {
        X509AuthenticationConfig config = X509AuthenticationConfig.getInstance();
        config.setClientCertIdType("SAN");
        config.setClientCertIdSanMatchType("6");
        TestCertificate certWithoutSan = new TestCertificate("CLIENT") {
            @Override
            public Collection<List<?>> getSubjectAlternativeNames() {
                return null;
            }
        };
        ClientIdentity identity = X509AuthenticationUtil.getClientId(certWithoutSan);
        assertEquals(CertificateType.SUBJECT_DN, identity.getCertificateType());
        assertEquals("CN=CLIENT", identity.getId());
    }

    @Test
    public void testSpiffeIdentityMatchesLegacySuperUserAndAcl() {
        System.setProperty("zookeeper.X509AuthenticationProvider.superUser", "servicePrincipal(kafka");
        TestCertificate spiffeCert = new TestCertificate("CLIENT", SPIFFE_KAFKA_SAN);
        X509AuthenticationProvider provider = createProvider(spiffeCert);
        MockServerCnxn cnxn = new MockServerCnxn();
        cnxn.clientChain = new X509Certificate[]{spiffeCert};

        assertEquals(KeeperException.Code.OK, provider.handleAuthentication(cnxn, null));
        assertEquals(new HashSet<>(Arrays.asList(new Id("super", "servicePrincipal(kafka"),
            new Id("x509", "application/example-mp/kafka"))), new HashSet<>(cnxn.getAuthInfo()));
        assertEquals(CertificateType.SPIFFE_V2, cnxn.getX509ClientIdentity().getCertificateType());

        ServerAuthenticationProvider.ServerObjs serverObjs = new ServerAuthenticationProvider.ServerObjs(null, cnxn);
        assertTrue(provider.matches(serverObjs, "application/example-mp/kafka", "kafka"));
        assertTrue(provider.matches(serverObjs, "application/example-mp/kafka",
            "urn:li:servicePrincipal(kafka;region1;instance1)"));
        assertFalse(provider.matches(serverObjs, "application/example-mp/kafka", "servicePrincipal(kafka-tools"));
        assertFalse(provider.matches(serverObjs, "application/example-mp/kafka", "Kafka"));
        assertFalse(provider.matches("application/example-mp/kafka", "kafka"));
    }

    @Test
    public void testLegacyIdentityDoesNotMatchSpiffeOnlyAcl() {
        TestCertificate legacyCert = new TestCertificate("CLIENT", "urn:li:servicePrincipal(kafka;region1;instance1)");
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_TYPE, "SAN");
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_TYPE, "6");
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_REGEX,
            "^.*urn:li:servicePrincipal\\(.*$");
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_REGEX,
            "^.*urn:li:([a-z]+Principal\\([^;%:]+)");
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_MATCHER_GROUP_INDEX, "1");
        X509AuthenticationProvider provider = createProvider(legacyCert);
        MockServerCnxn cnxn = new MockServerCnxn();
        cnxn.clientChain = new X509Certificate[]{legacyCert};

        assertEquals(KeeperException.Code.OK, provider.handleAuthentication(cnxn, null));
        assertEquals("servicePrincipal(kafka", cnxn.getAuthInfo().get(0).getId());
        assertEquals(CertificateType.LEGACY_SAN, cnxn.getX509ClientIdentity().getCertificateType());
        ServerAuthenticationProvider.ServerObjs serverObjs = new ServerAuthenticationProvider.ServerObjs(null, cnxn);
        assertFalse(provider.matches(serverObjs, "servicePrincipal(kafka", "application/example-mp/kafka"));
        assertFalse(provider.matches(serverObjs, "servicePrincipal(kafka", "kafka"));
        assertTrue(provider.matches(serverObjs, "servicePrincipal(kafka", "servicePrincipal(kafka"));
    }

    @Test
    public void testFailedAuthenticationClearsTypedIdentity() {
        TestCertificate spiffeCert = new TestCertificate("CLIENT", SPIFFE_KAFKA_SAN);
        MockServerCnxn cnxn = new MockServerCnxn();
        cnxn.clientChain = new X509Certificate[]{spiffeCert};
        assertEquals(KeeperException.Code.OK, createProvider(spiffeCert).handleAuthentication(cnxn, null));
        ClientIdentity identity = cnxn.getX509ClientIdentity();
        assertEquals(CertificateType.SPIFFE_V2, identity.getCertificateType());

        assertEquals(KeeperException.Code.AUTHFAILED, createProvider(unknownCert).handleAuthentication(cnxn, null));
        assertNull(cnxn.getX509ClientIdentity());
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
