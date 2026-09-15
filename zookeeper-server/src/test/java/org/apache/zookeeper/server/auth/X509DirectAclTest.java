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

package org.apache.zookeeper.server.auth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.common.SpiffeAuthTestUtil;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.MockServerCnxn;
import org.apache.zookeeper.server.PrepRequestProcessor;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.auth.znode.groupacl.X509ZNodeGroupAclProvider;
import org.apache.zookeeper.test.X509AuthTest.TestTrustManager;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class X509DirectAclTest extends ZKTestCase {
    private static final String PROVIDER_PROPERTY = ProviderRegistry.AUTHPROVIDER_PROPERTY_PREFIX + "x509";
    private static final String SUPERUSER_PROPERTY = "zookeeper.X509AuthenticationProvider.superUser";
    private static final String[] PROPERTIES = {
        PROVIDER_PROPERTY,
        SUPERUSER_PROPERTY,
        X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_TYPE,
        X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_TYPE,
        X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_REGEX,
        X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_REGEX,
        X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_MATCHER_GROUP_INDEX
    };

    private final Map<String, String> originalProperties = new HashMap<>();
    private ZooKeeperServer server;

    @BeforeClass
    public static void registerBouncyCastle() {
        SpiffeAuthTestUtil.registerBouncyCastle();
    }

    @Before
    public void setUp() {
        for (String property : PROPERTIES) {
            originalProperties.put(property, System.getProperty(property));
            System.clearProperty(property);
        }
        X509AuthenticationConfig.reset();
        selectProvider(X509AuthenticationProvider.class);
        server = new ZooKeeperServer();
    }

    @After
    public void tearDown() {
        for (String property : PROPERTIES) {
            String value = originalProperties.get(property);
            if (value == null) {
                System.clearProperty(property);
            } else {
                System.setProperty(property, value);
            }
        }
        ProviderRegistry.reset();
        X509AuthenticationConfig.reset();
    }

    @Test
    public void testSpiffeWorkloadMatchesLegacyDirectAclsWithoutChangingAuthInfo() throws Exception {
        MockServerCnxn cnxn = authenticate("spiffe://example.org/v1/wl/kafka");
        assertEquals(Collections.singletonList(new Id("x509", "kafka")), cnxn.getAuthInfo());
        for (String id : Arrays.asList(
            "kafka", "servicePrincipal(kafka", "servicePrincipal(kafka)",
            "urn:li:servicePrincipal(kafka;region1;instance1)")) {
            server.checkACL(cnxn, acl(id, ZooDefs.Perms.READ), ZooDefs.Perms.READ,
                cnxn.getAuthInfo(), "/protected", null);
        }
        assertEquals(Collections.singletonList(new ACL(ZooDefs.Perms.ALL, new Id("x509", "kafka"))),
            PrepRequestProcessor.fixupACL("/created", cnxn.getAuthInfo(), ZooDefs.Ids.CREATOR_ALL_ACL));
    }

    @Test
    public void testAclPermissionAndExactApplicationNameAreStillRequired() throws Exception {
        MockServerCnxn cnxn = authenticate("spiffe://example.org/v1/wl/kafka");
        assertDenied(cnxn, "servicePrincipal(kafka", ZooDefs.Perms.READ, ZooDefs.Perms.WRITE);
        for (String id : Arrays.asList(
            "servicePrincipal(other", "servicePrincipal(kafka-extra", "servicePrincipal(Kafka",
            "userPrincipal(kafka", "groupPrincipal(kafka", "servicePrincipalMetadata(kafka)",
            "servicePrincipal(kafka)extra", "nested/servicePrincipal(kafka")) {
            assertDenied(cnxn, id, ZooDefs.Perms.ALL, ZooDefs.Perms.READ);
        }
    }

    @Test
    public void testOtherSpiffeTypesDoNotGainLegacyServiceAccess() throws Exception {
        for (String path : Arrays.asList(
            "/v2/kafka", "/v1/application/example-mp/kafka", "/v2/application/example-mp/kafka",
            "/v2/group/kafka", "/v1/user/kafka", "/v2/user/kafka",
            "/v2/%75ser/kafka", "/v1/wl/kafka/extra")) {
            MockServerCnxn cnxn = authenticate("spiffe://example.org" + path);
            assertDenied(cnxn, "servicePrincipal(kafka", ZooDefs.Perms.ALL, ZooDefs.Perms.READ);
        }
    }

    @Test
    public void testConfiguredSanWithSameBareNameDoesNotGainSpiffeCompatibility() throws Exception {
        configureSan("^urn:example:(.*)$");
        MockServerCnxn cnxn = authenticate("urn:example:kafka");
        assertEquals(Collections.singletonList(new Id("x509", "kafka")), cnxn.getAuthInfo());
        server.checkACL(cnxn, acl("kafka", ZooDefs.Perms.READ), ZooDefs.Perms.READ,
            cnxn.getAuthInfo(), "/protected", null);
        assertDenied(cnxn, "servicePrincipal(kafka", ZooDefs.Perms.ALL, ZooDefs.Perms.READ);
    }

    @Test
    public void testSubjectDnAndLegacySanExactAclsRemainValid() throws Exception {
        MockServerCnxn subject = authenticate("urn:example:kafka");
        server.checkACL(subject, acl("CN=test-client", ZooDefs.Perms.READ), ZooDefs.Perms.READ,
            subject.getAuthInfo(), "/protected", null);
        assertDenied(subject, "servicePrincipal(CN=test-client", ZooDefs.Perms.ALL, ZooDefs.Perms.READ);

        configureSan("^urn:li:(servicePrincipal\\([^;]+)");
        MockServerCnxn legacy = authenticate("urn:li:servicePrincipal(kafka;region1;instance1)");
        assertEquals(Collections.singletonList(new Id("x509", "servicePrincipal(kafka")), legacy.getAuthInfo());
        server.checkACL(legacy, acl("servicePrincipal(kafka", ZooDefs.Perms.READ), ZooDefs.Perms.READ,
            legacy.getAuthInfo(), "/protected", null);
        server.checkACL(legacy, acl("kafka", ZooDefs.Perms.READ), ZooDefs.Perms.READ,
            legacy.getAuthInfo(), "/protected", null);
        assertDenied(legacy, "servicePrincipal(other", ZooDefs.Perms.ALL, ZooDefs.Perms.READ);
        assertDenied(legacy, "kafka", ZooDefs.Perms.READ, ZooDefs.Perms.WRITE);
    }

    @Test
    public void testReverseCompatibilityRequiresLegacyServiceIdentity() throws Exception {
        configureSan("^urn:li:([^;]+)");
        for (String kind : Arrays.asList("userPrincipal", "groupPrincipal", "servicePrincipalMetadata")) {
            MockServerCnxn cnxn = authenticate("urn:li:" + kind + "(kafka;region1;instance1)");
            assertDenied(cnxn, "kafka", ZooDefs.Perms.ALL, ZooDefs.Perms.READ);
        }
        MockServerCnxn spiffe = authenticate("spiffe://example.org/v2/servicePrincipal(kafka");
        assertDenied(spiffe, "kafka", ZooDefs.Perms.ALL, ZooDefs.Perms.READ);
    }

    @Test
    public void testGroupProviderCannotTreatMappedDomainAsCertificateIdentity() throws Exception {
        selectProvider(X509ZNodeGroupAclProvider.class);
        assertFalse(ProviderRegistry.getServerProvider("x509").matches(
            null, new ServerAuthenticationProvider.MatchValues(
                "/protected", "kafka", "servicePrincipal(kafka", ZooDefs.Perms.READ, null)));
        MockServerCnxn cnxn = authenticate("spiffe://example.org/v1/wl/kafka");
        server.checkACL(cnxn, acl("servicePrincipal(kafka", ZooDefs.Perms.READ), ZooDefs.Perms.READ,
            cnxn.getAuthInfo(), "/protected", null);

        cnxn.addAuthInfo(new Id("x509", "other-domain"));
        server.checkACL(cnxn, acl("other-domain", ZooDefs.Perms.READ), ZooDefs.Perms.READ,
            cnxn.getAuthInfo(), "/protected", null);
        assertDenied(cnxn, "servicePrincipal(other-domain", ZooDefs.Perms.ALL, ZooDefs.Perms.READ);

        configureSan("^urn:li:(servicePrincipal\\([^;]+)");
        MockServerCnxn legacy = authenticate("urn:li:servicePrincipal(kafka;region1;instance1)");
        legacy.addAuthInfo(new Id("x509", "servicePrincipal(other"));
        server.checkACL(legacy, acl("kafka", ZooDefs.Perms.READ), ZooDefs.Perms.READ,
            legacy.getAuthInfo(), "/protected", null);
        assertDenied(legacy, "other", ZooDefs.Perms.ALL, ZooDefs.Perms.READ);
    }

    @Test
    public void testUnauthenticatedConnectionCannotEnableCompatibility() throws Exception {
        MockServerCnxn missing = new MockServerCnxn();
        missing.addAuthInfo(new Id("x509", "kafka"));
        assertDenied(missing, "servicePrincipal(kafka", ZooDefs.Perms.ALL, ZooDefs.Perms.READ);

        MockServerCnxn unsupported = new MockServerCnxn() {
            @Override
            public Certificate[] getClientCertificateChain() {
                throw new UnsupportedOperationException("No TLS certificate support");
            }
        };
        unsupported.addAuthInfo(new Id("x509", "kafka"));
        assertDenied(unsupported, "servicePrincipal(kafka", ZooDefs.Perms.ALL, ZooDefs.Perms.READ);

        MockServerCnxn certificateOnly = new MockServerCnxn();
        certificateOnly.clientChain = new X509Certificate[]{
            SpiffeAuthTestUtil.buildClientCertWithUriSans("spiffe://example.org/v1/wl/kafka")
        };
        certificateOnly.addAuthInfo(new Id("x509", "kafka"));
        assertDenied(certificateOnly, "servicePrincipal(kafka", ZooDefs.Perms.ALL, ZooDefs.Perms.READ);
    }

    @Test
    public void testFailedAuthenticationClearsCompatibilityIdentity() throws Exception {
        MockServerCnxn cnxn = authenticate("spiffe://example.org/v1/wl/kafka");
        X509Certificate differentCert =
            SpiffeAuthTestUtil.buildClientCertWithUriSans("spiffe://example.org/v1/wl/other");
        X509AuthenticationProvider provider = new X509AuthenticationProvider(
            new TestTrustManager(differentCert), new SpiffeAuthTestUtil.NoopKeyManager());

        assertEquals(KeeperException.Code.AUTHFAILED, provider.handleAuthentication(cnxn, null));
        assertNull(cnxn.getX509ClientIdentity());
        assertDenied(cnxn, "servicePrincipal(kafka", ZooDefs.Perms.ALL, ZooDefs.Perms.READ);
    }

    @Test
    public void testCompatibilityDoesNotPromoteConfiguredSuperuserAlias() throws Exception {
        System.setProperty(SUPERUSER_PROPERTY, "servicePrincipal(kafka");
        MockServerCnxn cnxn = authenticate("spiffe://example.org/v1/wl/kafka");
        assertEquals(Collections.singletonList(new Id("x509", "kafka")), cnxn.getAuthInfo());
        assertDenied(cnxn, "servicePrincipal(other", ZooDefs.Perms.ALL, ZooDefs.Perms.READ);
    }

    @Test
    public void testLegacyProviderWrapperKeepsExistingStringMatcher() {
        ServerAuthenticationProvider provider = ProviderRegistry.getServerProvider("ip");
        assertTrue(provider.matches(null, new ServerAuthenticationProvider.MatchValues(
            "/protected", "10.1.2.3", "10.0.0.0/8", ZooDefs.Perms.READ, null)));
        assertFalse(provider.matches(null, new ServerAuthenticationProvider.MatchValues(
            "/protected", "192.0.2.1", "10.0.0.0/8", ZooDefs.Perms.READ, null)));
    }

    private MockServerCnxn authenticate(String... uriSans) throws Exception {
        X509Certificate cert = SpiffeAuthTestUtil.buildClientCertWithUriSans(uriSans);
        X509AuthenticationProvider provider = new X509AuthenticationProvider(
            new SpiffeAuthTestUtil.AcceptAllTrustManager(), new SpiffeAuthTestUtil.NoopKeyManager());
        MockServerCnxn cnxn = new MockServerCnxn();
        cnxn.clientChain = new X509Certificate[]{cert};
        assertEquals(KeeperException.Code.OK, provider.handleAuthentication(cnxn, null));
        return cnxn;
    }

    private void assertDenied(MockServerCnxn cnxn, String id, int allowedPerms, int requestedPerm) {
        try {
            server.checkACL(cnxn, acl(id, allowedPerms), requestedPerm, cnxn.getAuthInfo(), "/protected", null);
            fail("Unexpected access to ACL " + id);
        } catch (KeeperException.NoAuthException expected) {
            // Expected denial.
        }
    }

    private static List<ACL> acl(String id, int perms) {
        return Collections.singletonList(new ACL(perms, new Id("x509", id)));
    }

    private static void selectProvider(Class<? extends AuthenticationProvider> providerClass) {
        System.setProperty(PROVIDER_PROPERTY, providerClass.getName());
        ProviderRegistry.reset();
    }

    private static void configureSan(String extractRegex) {
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_TYPE, "SAN");
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_TYPE, "6");
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_REGEX, "^urn:");
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_REGEX, extractRegex);
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_MATCHER_GROUP_INDEX, "1");
        X509AuthenticationConfig.reset();
    }
}
