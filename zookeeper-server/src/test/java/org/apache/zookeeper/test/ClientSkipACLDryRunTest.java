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

package org.apache.zookeeper.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.common.SpiffeAuthTestUtil;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.MockServerCnxn;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.auth.AuthenticationProvider;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.apache.zookeeper.server.auth.ProviderRegistry;
import org.apache.zookeeper.server.auth.X509AuthenticationConfig;
import org.apache.zookeeper.server.auth.X509AuthenticationProvider;
import org.apache.zookeeper.server.auth.X509QuorumAuthInfo;
import org.apache.zookeeper.server.auth.znode.groupacl.X509ZNodeGroupAclProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Like skipACL=yes, dryrun allows unauthorized operations, but it still evaluates
 * the ACL and warns on failure. Surefire runs each ClientTest subclass in its own JVM.
 */
public class ClientSkipACLDryRunTest extends ClientTest {

    private static final String FAILURE = "ACL check would have failed, request is allowed.";
    private static final long SESSION_ID = 0x1234abcdL;
    private static String originalSkipACL;

    @BeforeAll
    public static void setup() {
        originalSkipACL = System.getProperty(ZooKeeperServer.SKIP_ACL);
        System.setProperty(ZooKeeperServer.SKIP_ACL, ZooKeeperServer.SKIP_ACL_DRYRUN);
        SpiffeAuthTestUtil.registerBouncyCastle();
    }

    @AfterAll
    public static void teardown() {
        restoreProperty(ZooKeeperServer.SKIP_ACL, originalSkipACL);
    }

    @Test
    public void testFailedACLCheckIsLoggedAndAllowed() throws Exception {
        try (ZooKeeper owner = createClient(); ZooKeeper other = createClient();
             LoggerTestTool logger = new LoggerTestTool(ZooKeeperServer.class)) {
            owner.addAuthInfo("digest", "owner:password".getBytes(StandardCharsets.UTF_8));
            other.addAuthInfo("digest", "owner:wrong-password".getBytes(StandardCharsets.UTF_8));
            owner.create("/dryruntest", new byte[]{1}, ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
            assertArrayEquals(new byte[]{1}, owner.getData("/dryruntest", false, null));
            owner.setData("/dryruntest", new byte[]{2}, -1);
            assertFalse(logger.getOutputStream().toString().contains(FAILURE));
            logger.getOutputStream().reset();

            assertArrayEquals(new byte[]{2}, other.getData("/dryruntest", false, null));
            other.setData("/dryruntest", new byte[]{3}, -1);
            assertArrayEquals(new byte[]{3}, owner.getData("/dryruntest", false, null));
            String log = logger.getOutputStream().toString();
            for (int permission : Arrays.asList(Perms.READ, Perms.WRITE)) {
                assertDiagnostic(log, "/dryruntest", permission, other.getSessionId());
            }
            assertTrue(log.contains("clientAddress=/127.0.0.1:"));
            assertTrue(log.contains("digest:owner"));
            assertTrue(log.contains("nodeACL=[digest:owner:" + Perms.ALL + "]"));
            assertFalse(log.contains(DigestAuthenticationProvider.generateDigest("owner:password")));
            assertFalse(log.contains(DigestAuthenticationProvider.generateDigest("owner:wrong-password")));
        }
    }

    @Test
    public void testIdentityAwareChecksAndForwardedDiagnostics() throws Exception {
        String providerProperty = ProviderRegistry.AUTHPROVIDER_PROPERTY_PREFIX + "x509";
        String originalProvider = System.getProperty(providerProperty);
        String typeProperty = X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_TYPE;
        String originalType = System.getProperty(typeProperty);
        String matchTypeProperty = X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_TYPE;
        String originalMatchType = System.getProperty(matchTypeProperty);
        try {
            System.setProperty(typeProperty, "SAN");
            System.setProperty(matchTypeProperty, "6");
            X509AuthenticationConfig.reset();
            ZooKeeperServer server = new ZooKeeperServer();
            MockServerCnxn spiffe = authenticate("spiffe://example.org/v2/application/example-mp/orders");
            MockServerCnxn legacy = authenticate("urn:li:servicePrincipal(orders)");
            for (Class<? extends AuthenticationProvider> provider : Arrays.asList(
                X509AuthenticationProvider.class, X509ZNodeGroupAclProvider.class)) {
                System.setProperty(providerProperty, provider.getName());
                ProviderRegistry.reset();
                for (boolean forwarded : Arrays.asList(false, true)) {
                    Request request = request(spiffe, forwarded);
                    Request legacyRequest = request(legacy, forwarded);
                    for (int permission : Arrays.asList(Perms.READ, Perms.WRITE, Perms.CREATE, Perms.DELETE, Perms.ADMIN)) {
                        assertIdentityCheck(server, request, "servicePrincipal(orders", permission, false);
                        assertIdentityCheck(server, request, legacy.getX509ClientIdentity().getId(), permission, false);
                        assertIdentityCheck(server, request, "servicePrincipal(other", permission, true);
                        assertIdentityCheck(server, legacyRequest, legacy.getX509ClientIdentity().getId(), permission, false);
                        assertIdentityCheck(server, legacyRequest, spiffe.getX509ClientIdentity().getId(), permission, true);
                    }
                    Request missing = new Request(null, SESSION_ID, 1, ZooDefs.OpCode.setData, null, spiffe.getAuthInfo());
                    assertIdentityCheck(server, missing, "servicePrincipal(orders", Perms.WRITE, true);
                    assertIdentityCheck(server, missing, spiffe.getX509ClientIdentity().getId(), Perms.WRITE, false);
                }
                try (LoggerTestTool logger = new LoggerTestTool(ZooKeeperServer.class)) {
                    server.checkACL(spiffe, acl("x509", "servicePrincipal(orders", Perms.ALL),
                        Perms.WRITE, spiffe.getAuthInfo(), "/direct", null);
                    assertFalse(logger.getOutputStream().toString().contains(FAILURE));
                    server.checkACL(spiffe, acl("x509", "servicePrincipal(other", Perms.ALL),
                        Perms.WRITE, spiffe.getAuthInfo(), "/direct", null);
                    assertDiagnostic(logger.getOutputStream().toString(), "/direct", Perms.WRITE, spiffe.getSessionId());
                }
            }
        } finally {
            restoreProperty(providerProperty, originalProvider);
            restoreProperty(typeProperty, originalType);
            restoreProperty(matchTypeProperty, originalMatchType);
            ProviderRegistry.reset();
            X509AuthenticationConfig.reset();
        }
    }

    @Test
    public void testDiagnosticDescriptionsDoNotExposeUnknownCredentials() throws Exception {
        List<Id> ids = Arrays.asList(new Id("unknown", "private-credential"), new Id("x509", null));
        List<ACL> acls = Arrays.asList(
            new ACL(0, new Id("world", "anyone")),
            new ACL(0, new Id("auth", "")),
            new ACL(Perms.WRITE, new Id("unknown", "private-acl")),
            new ACL(0, new Id("x509", null)));
        try (LoggerTestTool logger = new LoggerTestTool(ZooKeeperServer.class)) {
            new ZooKeeperServer().checkACL((ServerCnxn) null, acls, Perms.WRITE, ids, "/descriptions", null);
            String log = logger.getOutputStream().toString();
            assertDiagnostic(log, "/descriptions", Perms.WRITE, -1);
            assertTrue(log.contains("clientAuth=[unknown, x509]"), log);
            assertTrue(log.contains("nodeACL=[world:anyone:0, auth::0, unknown:2, x509:0]"), log);
            assertFalse(log.contains("private-credential"), log);
            assertFalse(log.contains("private-acl"), log);
        }
    }

    @ParameterizedTest
    @MethodSource("configurations")
    public void testConfigurationInFreshJvm(String value, String mode) throws Exception {
        // skipACL is static final: changing properties in this test JVM cannot test initialization.
        List<String> command = new ArrayList<>();
        command.add(Paths.get(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-Djava.io.tmpdir=" + System.getProperty("java.io.tmpdir"));
        command.add("-cp");
        command.add(System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")));
        command.add(ConfigurationProbe.class.getName());
        command.add(mode);
        if (value != null) {
            command.add(value);
        }
        Path output = Paths.get("target", "surefire-reports", "skip-acl-config-"
            + (value == null ? "default" : Integer.toHexString(value.hashCode())) + ".log");
        Files.createDirectories(output.getParent());
        Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(output.toFile()).start();
        try {
            assertTrue(process.waitFor(60, TimeUnit.SECONDS), "Configuration probe timed out: " + command);
            String log = new String(Files.readAllBytes(output), StandardCharsets.UTF_8);
            assertEquals(0, process.exitValue(), log);
            assertTrue(log.contains("CONFIGURATION PASS: " + mode), log);
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                assertTrue(process.waitFor(10, TimeUnit.SECONDS), "Configuration probe failed to stop");
            }
        }
    }

    private static Stream<Arguments> configurations() {
        return Stream.of(
            Arguments.of(null, "ENFORCE"),
            Arguments.of("no", "ENFORCE"),
            Arguments.of("yes", "BYPASS"),
            Arguments.of("dryrun", "DRYRUN"),
            Arguments.of("", "ENFORCE"),
            Arguments.of("YES", "ENFORCE"),
            Arguments.of("DRYRUN", "ENFORCE"),
            Arguments.of("true", "ENFORCE"),
            Arguments.of(" yes", "ENFORCE"),
            Arguments.of("dryrun ", "ENFORCE"),
            Arguments.of("invalid", "ENFORCE"));
    }

    private static void assertIdentityCheck(ZooKeeperServer server, Request request, String id, int permission,
                                            boolean denied) throws Exception {
        try (LoggerTestTool logger = new LoggerTestTool(ZooKeeperServer.class)) {
            server.checkACL(request, acl("x509", id, permission), permission, "/identity", null);
            String log = logger.getOutputStream().toString();
            assertEquals(denied, log.contains(FAILURE), log);
            if (denied) {
                assertDiagnostic(log, "/identity", permission, SESSION_ID);
                if (request.cnxn == null) {
                    assertTrue(log.contains("clientAddress=null"), log);
                }
                assertTrue(log.contains("clientAuth=[x509:" + request.authInfo.get(0).getId() + "]"), log);
                assertTrue(log.contains("nodeACL=[x509:" + id + ":" + permission + "]"), log);
            }
        }
    }

    private static MockServerCnxn authenticate(String uri) throws Exception {
        X509Certificate cert = SpiffeAuthTestUtil.buildClientCertWithUriSans(uri);
        X509AuthenticationProvider provider = new X509AuthenticationProvider(
            new X509AuthTest.TestTrustManager(cert), new SpiffeAuthTestUtil.NoopKeyManager());
        MockServerCnxn cnxn = new MockServerCnxn();
        cnxn.clientChain = new X509Certificate[]{cert};
        assertEquals(KeeperException.Code.OK, provider.handleAuthentication(cnxn, null));
        return cnxn;
    }

    private static Request request(MockServerCnxn cnxn, boolean forwarded) throws Exception {
        if (!forwarded) {
            return new Request(cnxn, SESSION_ID, 1, ZooDefs.OpCode.setData, null, cnxn.getAuthInfo());
        }
        X509QuorumAuthInfo auth = X509QuorumAuthInfo.decode(
            X509QuorumAuthInfo.encode(cnxn.getAuthInfo(), cnxn.getX509ClientIdentity()));
        return new Request(null, SESSION_ID, 1, ZooDefs.OpCode.setData, null, auth.getAuthInfo(), auth.getClientIdentity());
    }

    private static void assertDiagnostic(String log, String path, int permission, long sessionId) {
        assertTrue(log.contains("WARN"), log);
        assertTrue(log.contains("zookeeper.skipACL=\"dryrun\": " + FAILURE
            + " path=" + path + ", requestedPerm=" + permission
            + ", sessionId=0x" + Long.toHexString(sessionId) + ", clientAddress="), log);
    }

    private static List<ACL> acl(String scheme, String id, int permissions) {
        return Collections.singletonList(new ACL(permissions, new Id(scheme, id)));
    }

    private static void restoreProperty(String key, String value) {
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }

    public static class CountingDigestProvider extends DigestAuthenticationProvider {
        private static int checks;

        @Override
        public boolean matches(String id, String aclExpr) {
            checks++;
            return super.matches(id, aclExpr);
        }
    }

    public static class ConfigurationProbe {

        public static void main(String[] args) throws Exception {
            String mode = args[0];
            restoreProperty(ZooKeeperServer.SKIP_ACL, args.length == 1 ? null : args[1]);
            System.setProperty(ProviderRegistry.AUTHPROVIDER_PROPERTY_PREFIX + "digest", CountingDigestProvider.class.getName());
            try (LoggerTestTool logger = new LoggerTestTool(ZooKeeperServer.class)) {
                ZooKeeperServer server = new ZooKeeperServer();
                String startup = logger.getOutputStream().toString();
                assertEquals("BYPASS".equals(mode), startup.contains("zookeeper.skipACL==\"yes\", ACL checks will be skipped"));
                assertEquals("DRYRUN".equals(mode), startup.contains("zookeeper.skipACL==\"dryrun\", ACL checks will be evaluated"
                    + " but failures will only be logged, requests will not be rejected"));
                List<ACL> acl = acl("digest", DigestAuthenticationProvider.generateDigest("owner:password"), Perms.ALL);
                List<Id> owner = Collections.singletonList(acl.get(0).getId());
                List<Id> other = Collections.singletonList(
                    new Id("digest", DigestAuthenticationProvider.generateDigest("owner:wrong-password")));
                for (int attempt = 0; attempt < 2; attempt++) {
                    CountingDigestProvider.checks = 0;
                    logger.getOutputStream().reset();
                    server.checkACL((ServerCnxn) null, acl, Perms.WRITE, owner, "/config", null);
                    assertFalse(logger.getOutputStream().toString().contains(FAILURE));
                    if ("ENFORCE".equals(mode)) {
                        assertThrows(KeeperException.NoAuthException.class,
                            () -> server.checkACL((ServerCnxn) null, acl, Perms.WRITE, other, "/config", null));
                    } else {
                        server.checkACL((ServerCnxn) null, acl, Perms.WRITE, other, "/config", null);
                    }
                    assertEquals("BYPASS".equals(mode) ? 0 : 2, CountingDigestProvider.checks);
                    String log = logger.getOutputStream().toString();
                    assertEquals("DRYRUN".equals(mode), log.contains(FAILURE), log);
                    if ("DRYRUN".equals(mode)) {
                        assertDiagnostic(log, "/config", Perms.WRITE, -1);
                        assertTrue(log.contains("clientAddress=null, clientAuth=[digest:owner], nodeACL=[digest:owner:31]"), log);
                        assertFalse(log.contains(acl.get(0).getId().getId()), log);
                        assertFalse(log.contains(other.get(0).getId()), log);
                    }
                    System.setProperty(ZooKeeperServer.SKIP_ACL, "ENFORCE".equals(mode) ? "dryrun" : "no");
                }
                System.out.println("CONFIGURATION PASS: " + mode);
            }
        }
    }
}
