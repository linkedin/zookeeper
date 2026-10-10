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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.MockServerCnxn;
import org.apache.zookeeper.server.PrepRequestProcessor;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.auth.X509AuthenticationUtil.CertificateType;
import org.apache.zookeeper.server.auth.X509AuthenticationUtil.ClientIdentity;
import org.apache.zookeeper.server.auth.znode.groupacl.X509ZNodeGroupAclProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class X509CreatorAclTest extends ZKTestCase {
    private static final String PROVIDER_PROPERTY = ProviderRegistry.AUTHPROVIDER_PROPERTY_PREFIX + "x509";
    private static final String APP = "application/example-mp/kafka";
    private static final String LEGACY = "servicePrincipal(kafka";
    private static final String[] PROPERTIES = {
        PROVIDER_PROPERTY,
        X509AuthenticationConfig.SET_X509_CLIENT_ID_AS_ACL,
        X509AuthenticationConfig.ALLOWED_CLIENT_ID_AS_ACL_DOMAINS,
        X509AuthenticationConfig.DEDICATED_DOMAIN,
        X509AuthenticationConfig.ZOOKEEPER_ZNODEGROUPACL_SUPERUSER_ID,
        X509AuthenticationConfig.OPEN_READ_ACCESS_PATH_PREFIX
    };
    private final Map<String, String> originalProperties = new HashMap<>();

    @Before
    public void setUp() {
        for (String property : PROPERTIES) {
            originalProperties.put(property, System.getProperty(property));
            System.clearProperty(property);
        }
        System.setProperty(PROVIDER_PROPERTY, X509ZNodeGroupAclProvider.class.getName());
        System.setProperty(X509AuthenticationConfig.SET_X509_CLIENT_ID_AS_ACL, "true");
        ProviderRegistry.reset();
        X509AuthenticationConfig.reset();
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
    public void testAutomaticAclFormatsSpiffeApplicationsWithoutChangingAuthentication() throws Exception {
        for (CertificateType type : Arrays.asList(CertificateType.SPIFFE_V1_WL,
            CertificateType.SPIFFE_V1_WORKLOAD, CertificateType.SPIFFE_V2)) {
            List<String> ids = type == CertificateType.SPIFFE_V1_WL
                ? Collections.singletonList("kafka") : Arrays.asList(APP, APP + "/blue");
            for (String id : ids) {
                ClientIdentity identity = new ClientIdentity(type, id);
                List<Id> authInfo = Collections.singletonList(new Id("x509", id));
                for (List<ACL> requested : Arrays.asList(ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    ZooDefs.Ids.CREATOR_ALL_ACL, acl("unrelated", ZooDefs.Perms.READ))) {
                    assertEquals(acl(LEGACY, ZooDefs.Perms.ALL), fixup(identity, authInfo, requested));
                }
                assertEquals(Collections.singletonList(new Id("x509", id)), authInfo);
                assertEquals(id, identity.getId());
                assertEquals(type, identity.getCertificateType());
            }
        }
    }

    @Test
    public void testLegacyAndNonApplicationIdentitiesKeepOriginalAclIds() throws Exception {
        for (CertificateType type : Arrays.asList(CertificateType.LEGACY_SAN, CertificateType.SUBJECT_DN)) {
            for (String id : Arrays.asList("kafka", APP, LEGACY, "CN=admin")) {
                assertOriginalAcl(new ClientIdentity(type, id));
            }
        }
        for (CertificateType type : Arrays.asList(CertificateType.SPIFFE_V1_WORKLOAD, CertificateType.SPIFFE_V2)) {
            for (String id : Arrays.asList("kafka", "user/kafka", "group/kafka", "airflow/kafka",
                "application/kafka", "application//kafka", APP + "/", APP + "//tag", APP + "/tag/extra")) {
                assertOriginalAcl(new ClientIdentity(type, id));
            }
        }
    }

    @Test
    public void testUnrepresentableApplicationNamesKeepOriginalAclIds() throws Exception {
        for (String app : Arrays.asList("", "kafka)", "kafka;instance", "kafka(other", "kafka/extra")) {
            assertOriginalAcl(new ClientIdentity(CertificateType.SPIFFE_V1_WL, app));
        }
        for (String app : Arrays.asList("kafka)", "kafka;instance", "kafka(other")) {
            assertOriginalAcl(new ClientIdentity(CertificateType.SPIFFE_V2, "application/example-mp/" + app));
        }
    }

    @Test
    public void testFormattingUsesExistingFormattedPrincipalGrammar() throws Exception {
        for (String app : Arrays.asList("Kafka", "kafka-server", "kafka_1", "kafka.v2", "kafka+worker", "_kafka")) {
            ClientIdentity identity = new ClientIdentity(CertificateType.SPIFFE_V2, "application/example-mp/" + app);
            assertEquals(acl("servicePrincipal(" + app, ZooDefs.Perms.ALL),
                fixup(identity, authInfo(identity), ZooDefs.Ids.OPEN_ACL_UNSAFE));
        }
    }

    @Test
    public void testMappedDomainsRemainUnchangedAlongsideFormattedClient() throws Exception {
        ClientIdentity identity = applicationIdentity();
        List<Id> authInfo = Arrays.asList(new Id("x509", "broker-access"), new Id("x509", "kafka"),
            new Id("x509", APP));
        assertEquals(Arrays.asList(new ACL(ZooDefs.Perms.ALL, authInfo.get(0)),
            new ACL(ZooDefs.Perms.ALL, authInfo.get(1)), new ACL(ZooDefs.Perms.ALL, new Id("x509", LEGACY))),
            fixup(identity, authInfo, ZooDefs.Ids.OPEN_ACL_UNSAFE));
        assertEquals(APP, authInfo.get(2).getId());
    }

    @Test
    public void testDomainOnlyAuthInfoDoesNotAcquireClientAcl() throws Exception {
        List<Id> authInfo = Collections.singletonList(new Id("x509", "broker-access"));
        assertEquals(acl("broker-access", ZooDefs.Perms.ALL),
            fixup(applicationIdentity(), authInfo, ZooDefs.Ids.OPEN_ACL_UNSAFE));
    }

    @Test
    public void testFormattedClientAclDoesNotDuplicateAnIdenticalDomainGrant() throws Exception {
        List<Id> authInfo = Arrays.asList(new Id("x509", LEGACY), new Id("x509", APP));
        assertEquals(acl(LEGACY, ZooDefs.Perms.ALL),
            fixup(applicationIdentity(), authInfo, ZooDefs.Ids.OPEN_ACL_UNSAFE));
    }

    @Test
    public void testFormattingRequiresTypedContextBoundToAuthInfo() throws Exception {
        ClientIdentity identity = applicationIdentity();
        assertEquals(acl(APP, ZooDefs.Perms.ALL),
            fixup(null, authInfo(identity), ZooDefs.Ids.OPEN_ACL_UNSAFE));
        assertEquals(acl(APP, ZooDefs.Perms.ALL),
            PrepRequestProcessor.fixupACL("/created", authInfo(identity), ZooDefs.Ids.OPEN_ACL_UNSAFE));
        String otherId = "application/other-mp/kafka";
        assertEquals(acl(otherId, ZooDefs.Perms.ALL), fixup(identity,
            Collections.singletonList(new Id("x509", otherId)), ZooDefs.Ids.OPEN_ACL_UNSAFE));
    }

    @Test
    public void testDisabledAutomaticReplacementKeepsExplicitAclAndCreatorExpansion() throws Exception {
        System.setProperty(X509AuthenticationConfig.SET_X509_CLIENT_ID_AS_ACL, "false");
        ClientIdentity identity = applicationIdentity();
        List<ACL> requested = acl(APP, ZooDefs.Perms.READ);
        assertEquals(requested, fixup(identity, authInfo(identity), requested));
        assertEquals(acl(APP, ZooDefs.Perms.ALL),
            fixup(identity, authInfo(identity), ZooDefs.Ids.CREATOR_ALL_ACL));
    }

    @Test
    public void testDomainAllowlistWithFlagDisabledDoesNotFormatClientIds() throws Exception {
        System.setProperty(X509AuthenticationConfig.SET_X509_CLIENT_ID_AS_ACL, "false");
        System.setProperty(X509AuthenticationConfig.ALLOWED_CLIENT_ID_AS_ACL_DOMAINS, APP);
        assertOriginalAcl(applicationIdentity());
    }

    @Test
    public void testBasicProviderKeepsOriginalCreatorAcl() throws Exception {
        System.setProperty(PROVIDER_PROPERTY, X509AuthenticationProvider.class.getName());
        ProviderRegistry.reset();
        ClientIdentity identity = applicationIdentity();
        assertEquals(acl(APP, ZooDefs.Perms.ALL),
            fixup(identity, authInfo(identity), ZooDefs.Ids.CREATOR_ALL_ACL));
    }

    @Test
    public void testDedicatedServerKeepsOriginalCreatorAcl() throws Exception {
        System.setProperty(X509AuthenticationConfig.DEDICATED_DOMAIN, "broker-access");
        ClientIdentity identity = applicationIdentity();
        assertEquals(acl(APP, ZooDefs.Perms.ALL),
            fixup(identity, authInfo(identity), ZooDefs.Ids.CREATOR_ALL_ACL));
    }

    @Test
    public void testExplicitSuperuserKeepsRequestedAcl() throws Exception {
        System.setProperty(X509AuthenticationConfig.ZOOKEEPER_ZNODEGROUPACL_SUPERUSER_ID, LEGACY);
        List<Id> authInfo = Collections.singletonList(new Id("super", LEGACY));
        List<ACL> requested = acl(APP, ZooDefs.Perms.READ);
        assertEquals(requested, fixup(applicationIdentity(), authInfo, requested));
    }

    @Test
    public void testCrossDomainMarkerIsNotFormattedEvenWhenEqualToOriginalId() throws Exception {
        assertEquals(acl(APP, ZooDefs.Perms.ALL), fixup(applicationIdentity(),
            Collections.singletonList(new Id("super", APP)), ZooDefs.Ids.OPEN_ACL_UNSAFE));
    }

    @Test
    public void testOpenReadPolicyIsPreserved() throws Exception {
        System.setProperty(X509AuthenticationConfig.OPEN_READ_ACCESS_PATH_PREFIX, "/created");
        assertEquals(Arrays.asList(new ACL(ZooDefs.Perms.ALL, new Id("x509", LEGACY)),
            new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE)),
            fixup(applicationIdentity(), authInfo(applicationIdentity()), ZooDefs.Ids.OPEN_ACL_UNSAFE));
    }

    @Test
    public void testGeneratedAclAllowsCreatorAndLegacyClientButNotOtherApplication() throws Exception {
        ClientIdentity identity = applicationIdentity();
        List<ACL> generated = fixup(identity, authInfo(identity), ZooDefs.Ids.OPEN_ACL_UNSAFE);
        ZooKeeperServer server = new ZooKeeperServer();
        Request creator = new Request(null, 1, 1, ZooDefs.OpCode.setData, null, authInfo(identity), identity);
        Request legacy = new Request(null, 1, 1, ZooDefs.OpCode.setData, null,
            Collections.singletonList(new Id("x509", LEGACY)), new ClientIdentity(CertificateType.LEGACY_SAN, LEGACY));
        ClientIdentity otherProduct = new ClientIdentity(CertificateType.SPIFFE_V2, "application/other-mp/kafka/blue");
        Request sameApp = new Request(null, 1, 1, ZooDefs.OpCode.setData, null, authInfo(otherProduct), otherProduct);
        for (int permission : Arrays.asList(ZooDefs.Perms.READ, ZooDefs.Perms.WRITE,
            ZooDefs.Perms.CREATE, ZooDefs.Perms.DELETE, ZooDefs.Perms.ADMIN)) {
            server.checkACL(creator, generated, permission, "/created", null);
            server.checkACL(legacy, generated, permission, "/created", null);
            server.checkACL(sameApp, generated, permission, "/created", null);
        }
        ClientIdentity other = new ClientIdentity(CertificateType.SPIFFE_V2, "application/example-mp/reporting");
        Request denied = new Request(null, 1, 1, ZooDefs.OpCode.setData, null, authInfo(other), other);
        try {
            server.checkACL(denied, generated, ZooDefs.Perms.WRITE, "/created", null);
            fail("Unrelated application must not match the generated ACL");
        } catch (KeeperException.NoAuthException expected) {
            // Expected denial.
        }
    }

    @Test
    public void testForwardedIdentitySnapshotProducesSameAcl() throws Exception {
        ClientIdentity identity = applicationIdentity();
        MockServerCnxn cnxn = new MockServerCnxn();
        cnxn.setX509ClientIdentity(identity);
        Request local = new Request(cnxn, 1, 1, ZooDefs.OpCode.create, null, authInfo(identity));
        cnxn.setX509ClientIdentity(new ClientIdentity(CertificateType.SPIFFE_V2, "application/other-mp/reporting"));
        assertSame(identity, local.getX509ClientIdentity());
        X509QuorumAuthInfo decoded = X509QuorumAuthInfo.decode(
            X509QuorumAuthInfo.encode(local.authInfo, local.getX509ClientIdentity()));
        Request forwarded = new Request(null, 1, 1, ZooDefs.OpCode.create, null,
            decoded.getAuthInfo(), decoded.getClientIdentity());
        assertNull(forwarded.cnxn);
        for (Request request : Arrays.asList(local, forwarded)) {
            assertEquals(acl(LEGACY, ZooDefs.Perms.ALL),
                fixup(request.getX509ClientIdentity(), request.authInfo, ZooDefs.Ids.OPEN_ACL_UNSAFE));
            assertEquals(authInfo(identity), request.authInfo);
        }
    }

    @Test(expected = KeeperException.InvalidACLException.class)
    public void testEmptyRequestedAclIsStillRejected() throws Exception {
        fixup(applicationIdentity(), authInfo(applicationIdentity()), Collections.emptyList());
    }

    private static ClientIdentity applicationIdentity() {
        return new ClientIdentity(CertificateType.SPIFFE_V1_WORKLOAD, APP);
    }

    private static List<Id> authInfo(ClientIdentity identity) {
        return Collections.singletonList(new Id("x509", identity.getId()));
    }

    private static List<ACL> acl(String id, int permissions) {
        return Collections.singletonList(new ACL(permissions, new Id("x509", id)));
    }

    private static List<ACL> fixup(ClientIdentity identity, List<Id> authInfo, List<ACL> requested) throws Exception {
        return PrepRequestProcessor.fixupACL("/created", authInfo, requested, identity);
    }

    private static void assertOriginalAcl(ClientIdentity identity) throws Exception {
        assertEquals(acl(identity.getId(), ZooDefs.Perms.ALL),
            fixup(identity, authInfo(identity), ZooDefs.Ids.OPEN_ACL_UNSAFE));
    }
}
