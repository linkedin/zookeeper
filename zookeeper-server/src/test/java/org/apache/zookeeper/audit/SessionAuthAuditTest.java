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
package org.apache.zookeeper.audit;

import static org.apache.zookeeper.audit.AuditHelperTest.fields;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslServer;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.TestableZooKeeper;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.audit.AuditHelperTest.AuditCapture;
import org.apache.zookeeper.audit.AuditHelperTest.CredentialAuthenticationProvider;
import org.apache.zookeeper.audit.AuditHelperTest.FailingCounter;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.metrics.Counter;
import org.apache.zookeeper.proto.AuthPacket;
import org.apache.zookeeper.proto.GetSASLRequest;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;
import org.apache.zookeeper.proto.SetSASLResponse;
import org.apache.zookeeper.server.MockServerCnxn;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ZooKeeperSaslServer;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.apache.zookeeper.server.auth.ProviderRegistry;
import org.apache.zookeeper.server.auth.SASLAuthenticationProvider;
import org.apache.zookeeper.server.auth.X509AuthenticationProvider;
import org.apache.zookeeper.test.ClientBase;
import org.apache.zookeeper.test.QuorumUtil;
import org.apache.zookeeper.test.X509AuthTest.TestCertificate;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class SessionAuthAuditTest extends ZKTestCase {
    private static final long SESSION = 0x123;
    private static final int AUTH_XID = -4;
    private static final int SASL_XID = -33;
    private static final String SECRET = "SYNTHETIC_C2_PASSWORD";
    private static final String TOKEN = "SYNTHETIC_C2_TOKEN";
    private static final String CERTIFICATE = "SYNTHETIC_C2_CERTIFICATE_BODY";
    private static final String PROVIDER_PREFIX = "zookeeper.authProvider.auditC2";
    private final Map<String, String> previousProperties = new LinkedHashMap<>();
    private final List<String> captured = new ArrayList<>();
    private AuditCapture capture;
    private ZooKeeperServer server;
    private Object previousAuditEnabled;
    private Object previousAuditLogger;

    @Before
    public void setUp() throws Exception {
        property(ZKAuditProvider.AUDIT_ENABLE, "true");
        property(AuditHelperTest.ENHANCED_ENABLE, "true");
        property(ZooKeeperServer.ALLOW_SASL_FAILED_CLIENTS, "false");
        property(ZooKeeperServer.SESSION_REQUIRE_CLIENT_SASL_AUTH, "false");
        property(PROVIDER_PREFIX + "Sasl", SASLAuthenticationProvider.class.getName());
        property(PROVIDER_PREFIX + "X509", X509AuthenticationProvider.class.getName());
        property(PROVIDER_PREFIX + "Custom", CredentialAuthenticationProvider.class.getName());
        property(PROVIDER_PREFIX + "Empty", EmptyAuthenticationProvider.class.getName());
        ProviderRegistry.reset();
        ProviderRegistry.initialize();
        previousAuditEnabled = AuditHelperTest.replaceProviderField("auditEnabled", true);
        previousAuditLogger = AuditHelperTest.replaceProviderField("auditLogger", new Slf4jAuditLogger());
        capture = new AuditCapture();
        server = new ZooKeeperServer();
    }

    @After
    public void tearDown() throws Exception {
        try {
            for (String log : captured) {
                assertFalse(log, log.contains(SECRET));
                assertFalse(log, log.contains(TOKEN));
                assertFalse(log, log.contains(CERTIFICATE));
                assertFalse(log, log.contains(DigestAuthenticationProvider.generateDigest("alice:" + SECRET)));
            }
        } finally {
            capture.close();
            AuditHelperTest.replaceProviderField("auditLogger", previousAuditLogger);
            AuditHelperTest.replaceProviderField("auditEnabled", previousAuditEnabled);
            previousProperties.forEach(AuditHelperTest::restoreProperty);
            ProviderRegistry.reset();
        }
    }

    @Test
    public void testAnonymousAttachmentDoesNotClaimAuthenticationOrNewSession() throws Exception {
        RecordingCnxn connection = new RecordingCnxn(SESSION);
        server.finishSessionInit(connection, true);
        String first = read(1).get(0);
        assertBindingEvent(first, "sessionEstablished", "0x123", "success", null, "unknown", null);
        assertBindings(first, new String[][]{{null, null}});

        server.finishSessionInit(connection, true);
        String reconnect = read(1).get(0);
        assertBindingEvent(reconnect, "sessionEstablished", "0x123", "success", null, "unknown", null);
        assertBindings(reconnect, new String[][]{{null, null}});
    }

    @Test
    public void testRejectedAttachmentDoesNotBindAnUnvalidatedSessionId() {
        server.finishSessionInit(new RecordingCnxn(SESSION), false);
        read(0);
    }

    @Test
    public void testX509IdentityBeforeAssignmentIsBoundWithoutClaimingTls() throws Exception {
        RecordingCnxn connection = new RecordingCnxn(0);
        X509Certificate certificate = new TestCertificate("CLIENT,OU=Audit") {
            @Override
            public byte[] getEncoded() {
                return CERTIFICATE.getBytes(StandardCharsets.UTF_8);
            }
        };
        connection.clientChain = new X509Certificate[]{certificate};
        X509AuthenticationProvider provider = new X509AuthenticationProvider(mock(X509TrustManager.class), null);
        assertEquals(Code.OK, provider.handleAuthentication(connection, null));
        assertEquals(0, connection.getSessionId());
        read(0);

        connection.sessionId = SESSION;
        connection.addAuthInfo(new Id("super", TOKEN));
        server.finishSessionInit(connection, true);
        List<String> logs = read(2);
        assertBindings(logs, new String[][]{{"x509", "CN=CLIENT,OU=Audit"}, {"super", "[redacted]"}});
        assertFalse(connection.isSecure());
        for (String log : logs) {
            assertBindingEvent(log, "sessionEstablished", "0x123", "success", null, "unknown", null);
            assertNull(fields(log).get("tls"));
            assertNull(fields(log).get("secure"));
        }
    }

    @Test
    public void testMultipleSchemesRemainPairedAndUntrustedIdentitiesAreRedacted() throws Exception {
        RecordingCnxn connection = new RecordingCnxn(SESSION);
        connection.addAuthInfo(new Id("ip", "127.0.0.1"));
        connection.addAuthInfo(new Id("digest", DigestAuthenticationProvider.generateDigest("alice:" + SECRET)));
        connection.addAuthInfo(new Id("sasl", "service/host@EXAMPLE.COM"));
        connection.addAuthInfo(new Id("x509", "CN=Bob,OU=Audit"));
        connection.addAuthInfo(new Id("x509", "-----BEGIN CERTIFICATE-----\n" + CERTIFICATE));
        connection.addAuthInfo(new Id("audit-test-custom", "alice:" + SECRET));
        connection.addAuthInfo(new Id("missing", TOKEN));
        connection.addAuthInfo(new Id("digest", TOKEN));
        server.finishSessionInit(connection, true);
        List<String> logs = read(8);
        assertBindings(logs, new String[][]{
                {"ip", "127.0.0.1"}, {"digest", "alice"}, {"sasl", "service/host@EXAMPLE.COM"},
                {"x509", "CN=Bob,OU=Audit"}, {"audit-test-custom", "[redacted]"},
                {"x509", "[redacted]"},
                {"missing", "[redacted]"}, {"digest", "[redacted]"}});
        for (String log : logs) {
            assertBindingEvent(log, "sessionEstablished", "0x123", "success", null, "unknown", null);
        }
    }

    @Test
    public void testExplicitAuthRecordsOnlyTheAcceptedSchemeAndNotCredentials() throws Exception {
        RecordingCnxn connection = new RecordingCnxn(SESSION);
        connection.addAuthInfo(new Id("ip", "127.0.0.1"));
        authenticate(connection, "digest", "alice:" + SECRET);
        assertEquals(Code.OK.intValue(), connection.lastReply().getErr());
        String log = read(1).get(0);
        assertBindingEvent(log, "authentication", "0x123", "success", "0", "unknown", "-4");
        assertBindings(log, new String[][]{{"digest", "alice"}});
        assertTrue(connection.getAuthInfo().contains(
                new Id("digest", DigestAuthenticationProvider.generateDigest("alice:" + SECRET))));
    }

    @Test
    public void testPreSessionAuthFailureHasNoInventedSessionOrPrincipal() throws Exception {
        RecordingCnxn connection = new RecordingCnxn(0);
        authenticate(connection, "missing", TOKEN);
        assertEquals(Code.AUTHFAILED.intValue(), connection.lastReply().getErr());
        String log = read(1).get(0);
        assertBindingEvent(log, "authentication", null, "failure", "-115", "failed", "-4");
        assertBindings(log, new String[][]{{"missing", null}});
    }

    @Test
    public void testAuthFailureDoesNotBlameAPreviouslyAttachedPrincipal() throws Exception {
        RecordingCnxn connection = new RecordingCnxn(SESSION);
        connection.addAuthInfo(new Id("sasl", "previous@EXAMPLE.COM"));
        authenticate(connection, "sasl", TOKEN);
        assertEquals(Code.AUTHFAILED.intValue(), connection.lastReply().getErr());
        String log = read(1).get(0);
        assertBindings(log, new String[][]{{"sasl", null}});
        assertFalse(log.contains("previous@EXAMPLE.COM"));
    }

    @Test
    public void testAcceptedAuthWithoutAnIdentityRetainsUnknownPrincipal() throws Exception {
        RecordingCnxn connection = new RecordingCnxn(SESSION);
        connection.addAuthInfo(new Id("ip", "127.0.0.1"));
        authenticate(connection, "audit-c2-empty", TOKEN);
        assertEquals(Code.OK.intValue(), connection.lastReply().getErr());
        String log = read(1).get(0);
        assertBindingEvent(log, "authentication", "0x123", "success", "0", "unknown", "-4");
        assertBindings(log, new String[][]{{"audit-c2-empty", null}});
    }

    @Test
    public void testScalarUserRoundTripsDelimitersThroughRealSlf4jFormatting() throws Exception {
        String username = "a,b=\"c\\d\t\r\n";
        RecordingCnxn connection = new RecordingCnxn(SESSION);
        authenticate(connection, "digest", username + ":" + SECRET);
        String log = read(1).get(0);
        assertBindings(log, new String[][]{{"digest", "a,b=\"c\\d\t\r\n"}});
        assertFalse(log.contains("\n"));
        assertFalse(log.contains("\r"));
        assertEquals(10, fields(log).size());
    }

    @Test
    public void testAuthSchemeUsesTheExistingVersionedEscaping() throws Exception {
        RecordingCnxn connection = new RecordingCnxn(SESSION);
        authenticate(connection, "missing,scheme=\"value\\t\t\r\n", TOKEN);
        String log = read(1).get(0);
        assertBindingEvent(log, "authentication", "0x123", "failure", "-115", "failed", "-4");
        assertBindings(log, new String[][]{{"missing,scheme=\"value\\t\t\r\n", null}});
        assertFalse(log.contains("\n"));
        assertFalse(log.contains("\r"));
        assertFalse(fields(log).containsKey("user"));
    }

    @Test
    public void testRepeatedSanitizedPrincipalIsBoundOncePerAuthOutcome() throws Exception {
        RecordingCnxn connection = new RecordingCnxn(SESSION);
        authenticate(connection, "digest", "alice:" + SECRET);
        assertBindings(read(1).get(0), new String[][]{{"digest", "alice"}});
        authenticate(connection, "digest", "alice:" + TOKEN);
        assertEquals(2, connection.getAuthInfo().size());
        String log = read(1).get(0);
        assertBindingEvent(log, "authentication", "0x123", "success", "0", "unknown", "-4");
        assertBindings(log, new String[][]{{"digest", "alice"}});
    }

    @Test
    public void testRedactedIdentitiesDoNotProduceDuplicateBindings() throws Exception {
        RecordingCnxn connection = new RecordingCnxn(SESSION);
        connection.addAuthInfo(new Id("missing", SECRET));
        connection.addAuthInfo(new Id("missing", TOKEN));
        server.finishSessionInit(connection, true);
        assertBindings(read(1).get(0), new String[][]{{"missing", "[redacted]"}});
    }

    @Test
    public void testOneModeSnapshotCoversAllIdentityBindings() throws Exception {
        RecordingCnxn connection = new RecordingCnxn(SESSION);
        connection.addAuthInfo(new Id("ip", "127.0.0.1"));
        connection.addAuthInfo(new Id("digest", DigestAuthenticationProvider.generateDigest("alice:" + SECRET)));
        Slf4jAuditLogger logger = new Slf4jAuditLogger();
        AtomicInteger emitted = new AtomicInteger();
        Object previous = AuditHelperTest.replaceProviderField("auditLogger", (AuditLogger) event -> {
            logger.logAuditEvent(event);
            if (emitted.incrementAndGet() == 1) {
                System.setProperty(AuditHelperTest.ENHANCED_ENABLE, "false");
            }
        });
        try {
            server.finishSessionInit(connection, true);
            List<String> logs = read(2);
            assertBindings(logs, new String[][]{{"ip", "127.0.0.1"}, {"digest", "alice"}});
            for (String log : logs) {
                assertBindingEvent(log, "sessionEstablished", "0x123", "success", null, "unknown", null);
            }
            server.finishSessionInit(connection, true);
            read(0);
        } finally {
            AuditHelperTest.replaceProviderField("auditLogger", previous);
        }
    }

    @Test
    public void testSinkAndCounterFailuresDoNotDiscardRemainingBindings() throws Exception {
        RecordingCnxn connection = new RecordingCnxn(SESSION);
        connection.addAuthInfo(new Id("ip", "127.0.0.1"));
        connection.addAuthInfo(new Id("digest", DigestAuthenticationProvider.generateDigest("alice:" + SECRET)));
        FailingCounter counter = new FailingCounter();
        Counter previousCounter = AuditHelperTest.replaceAuditErrorCounter(counter);
        Slf4jAuditLogger logger = new Slf4jAuditLogger();
        Object previousLogger = AuditHelperTest.replaceProviderField("auditLogger", (AuditLogger) event -> {
            logger.logAuditEvent(event);
            throw new IllegalStateException("synthetic per-binding sink failure");
        });
        try {
            server.finishSessionInit(connection, true);
            assertNull(connection.disconnectReason);
            assertBindings(read(2), new String[][]{{"ip", "127.0.0.1"}, {"digest", "alice"}});
            assertEquals(2, counter.get());
        } finally {
            AuditHelperTest.replaceProviderField("auditLogger", previousLogger);
            AuditHelperTest.replaceAuditErrorCounter(previousCounter);
        }
    }

    @Test
    public void testDisabledGatesDoNotReadBindingMetadataOrChangeAuthentication() throws Exception {
        RecordingCnxn connection = new RecordingCnxn(SESSION);
        connection.authInfoRead = () -> {
            throw new IllegalStateException("binding metadata must not be read");
        };
        property(AuditHelperTest.ENHANCED_ENABLE, null);
        server.finishSessionInit(connection, true);
        authenticate(connection, "digest", "alice:" + SECRET);
        assertEquals(Code.OK.intValue(), connection.lastReply().getErr());
        read(0);

        property(AuditHelperTest.ENHANCED_ENABLE, "true");
        Object previous = AuditHelperTest.replaceProviderField("auditEnabled", false);
        try {
            server.finishSessionInit(connection, true);
            authenticate(connection, "digest", "alice:" + SECRET);
            assertEquals(Code.OK.intValue(), connection.lastReply().getErr());
            read(0);
        } finally {
            AuditHelperTest.replaceProviderField("auditEnabled", previous);
        }
    }

    @Test
    public void testBindingKeepsCapturedModeWhenIdentityExtractionChangesTheGate() throws Exception {
        RecordingCnxn connection = new RecordingCnxn(SESSION);
        connection.addAuthInfo(new Id("audit-test-custom", "alice:" + SECRET));
        connection.authInfoRead = () -> System.setProperty(AuditHelperTest.ENHANCED_ENABLE, "false");
        server.finishSessionInit(connection, true);
        String log = read(1).get(0);
        assertBindingEvent(log, "sessionEstablished", "0x123", "success", null, "unknown", null);
        assertBindings(log, new String[][]{{"audit-test-custom", "[redacted]"}});
        server.finishSessionInit(connection, true);
        read(0);
    }

    @Test
    public void testAuditGateReadFailureDoesNotAbortAcceptedAuthentication() throws Exception {
        RecordingCnxn connection = new RecordingCnxn(SESSION);
        long errors = AuditHelperTest.auditErrors();
        Properties previous = failEnhancedGateRead();
        try {
            try {
                authenticate(connection, "digest", "alice:" + SECRET);
            } catch (RuntimeException e) {
                fail("Audit gate access must not abort accepted authentication: " + e);
            }
            assertEquals(Code.OK.intValue(), connection.lastReply().getErr());
            assertEquals(errors + 1, AuditHelperTest.auditErrors());
            read(0);
        } finally {
            System.setProperties(previous);
        }
    }

    @Test
    public void testAuditGateReadFailureDoesNotCloseAValidSession() {
        RecordingCnxn connection = new RecordingCnxn(SESSION);
        long errors = AuditHelperTest.auditErrors();
        Properties previous = failEnhancedGateRead();
        try {
            server.finishSessionInit(connection, true);
            assertNull("Audit gate access must not close a valid session", connection.disconnectReason);
            assertEquals(errors + 1, AuditHelperTest.auditErrors());
            read(0);
        } finally {
            System.setProperties(previous);
        }
    }

    @Test
    public void testMetadataFailureAndBothBrokenReportersCannotChangeAuthOrSession() throws Exception {
        RecordingCnxn connection = new RecordingCnxn(SESSION);
        connection.authInfoRead = () -> {
            throw new IllegalStateException("synthetic binding metadata failure");
        };
        FailingCounter counter = new FailingCounter();
        Counter previousCounter = AuditHelperTest.replaceAuditErrorCounter(counter);
        Logger logger = (Logger) LoggerFactory.getLogger(AuditHelper.class);
        Level previousLevel = logger.getLevel();
        AtomicInteger reports = new AtomicInteger();
        AppenderBase<ILoggingEvent> failingAppender = new AppenderBase<ILoggingEvent>() {
            @Override
            public void doAppend(ILoggingEvent event) {
                reports.incrementAndGet();
                throw new IllegalStateException("synthetic diagnostic failure");
            }

            @Override
            protected void append(ILoggingEvent event) {
            }
        };
        logger.setLevel(Level.ERROR);
        logger.addAppender(failingAppender);
        try {
            server.finishSessionInit(connection, true);
            authenticate(connection, "digest", "alice:" + SECRET);
            assertNull(connection.disconnectReason);
            assertEquals(Code.OK.intValue(), connection.lastReply().getErr());
            assertEquals(2, counter.get());
            assertEquals(2, reports.get());
            read(0);
            connection.authInfoRead = null;
            assertTrue(connection.getAuthInfo().contains(
                    new Id("digest", DigestAuthenticationProvider.generateDigest("alice:" + SECRET))));
        } finally {
            logger.detachAppender(failingAppender);
            logger.setLevel(previousLevel);
            AuditHelperTest.replaceAuditErrorCounter(previousCounter);
        }
    }

    @Test
    public void testAuthenticationKeepsCapturedModeDuringBindingExtraction() throws Exception {
        RecordingCnxn connection = new RecordingCnxn(SESSION);
        connection.authInfoRead = () -> System.setProperty(AuditHelperTest.ENHANCED_ENABLE, "false");
        authenticate(connection, "audit-test-custom", "alice:" + SECRET);
        String log = read(1).get(0);
        assertBindingEvent(log, "authentication", "0x123", "success", "0", "unknown", "-4");
        assertBindings(log, new String[][]{{"audit-test-custom", "[redacted]"}});
        assertEquals(Code.OK.intValue(), connection.lastReply().getErr());
    }

    @Test
    public void testSaslCompletionDoesNotAuditIntermediateChallenges() throws Exception {
        RecordingCnxn connection = new RecordingCnxn(SESSION);
        try (SaslExchange sasl = new SaslExchange(connection, SECRET)) {
            byte[] response = sasl.challenge();
            assertFalse(sasl.server.isComplete());
            read(0);
            sasl.respond(response);
            assertTrue(sasl.server.isComplete());
            assertTrue(sasl.client.isComplete());
            assertEquals(Code.OK.intValue(), connection.lastReply().getErr());
            String log = read(1).get(0);
            assertBindingEvent(log, "authentication", "0x123", "success", "0", "unknown", "-33");
            assertBindings(log, new String[][]{{"sasl", "alice"}});
            assertTrue(connection.getAuthInfo().contains(new Id("sasl", "alice")));
        }
    }

    @Test
    public void testSaslFailurePreservesDefaultClosingPolicy() throws Exception {
        assertSaslFailure(SESSION, false, false, Code.AUTHFAILED, true);
    }

    @Test
    public void testAllowedSaslFailureIsNotMistakenForSuccessfulAuthentication() throws Exception {
        assertSaslFailure(SESSION, true, false, Code.OK, false);
    }

    @Test
    public void testRequiredSaslFailurePreservesItsDistinctClosingCode() throws Exception {
        assertSaslFailure(SESSION, true, true, Code.SESSIONCLOSEDREQUIRESASLAUTH, true);
    }

    @Test
    public void testPreSessionSaslFailureOmitsSessionIdentity() throws Exception {
        assertSaslFailure(0, false, false, Code.AUTHFAILED, true);
    }

    @Test
    public void testMissingSaslServerDoesNotFabricateAuthenticationSuccess() throws Exception {
        RecordingCnxn connection = new RecordingCnxn(SESSION);
        server.processPacket(connection, packet(OpCode.sasl, SASL_XID, new GetSASLRequest(new byte[0])));
        assertEquals(Code.OK.intValue(), connection.lastReply().getErr());
        read(0);
    }

    @Test
    public void testFailingAuditSinkAndCounterPreserveBothSaslFailurePolicies() throws Exception {
        FailingCounter counter = new FailingCounter();
        Counter previousCounter = AuditHelperTest.replaceAuditErrorCounter(counter);
        Slf4jAuditLogger logger = new Slf4jAuditLogger();
        Object previousLogger = AuditHelperTest.replaceProviderField("auditLogger", (AuditLogger) event -> {
            logger.logAuditEvent(event);
            throw new IllegalStateException("synthetic C2 SASL audit sink failure");
        });
        try {
            assertSaslFailure(SESSION, false, false, Code.AUTHFAILED, true);
            assertSaslFailure(SESSION, true, false, Code.OK, false);
            assertEquals(2, counter.get());
        } finally {
            AuditHelperTest.replaceProviderField("auditLogger", previousLogger);
            AuditHelperTest.replaceAuditErrorCounter(previousCounter);
        }
    }

    @Test
    public void testRealStandaloneAuthChangesDoNotRewriteEarlierWriteUsers() throws Exception {
        Standalone fixture = new Standalone();
        fixture.setUp();
        try {
            ZooKeeper client = fixture.connect();
            client.exists("/", false);
            String session = "0x" + Long.toHexString(client.getSessionId());
            String attachment = read(1).get(0);
            assertBindingEvent(attachment, "sessionEstablished", session, "success", null, "unknown", null);
            assertBindings(attachment, new String[][]{{"ip", "127.0.0.1"}});

            client.create("/before-auth", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            String before = read(1).get(0);
            assertEquals("127.0.0.1", fields(before).get("user"));
            client.addAuthInfo("digest", ("alice:" + SECRET).getBytes(StandardCharsets.UTF_8));
            client.exists("/", false);
            assertBindings(read(1).get(0), new String[][]{{"digest", "alice"}});
            client.setData("/before-auth", new byte[]{1}, -1);
            assertUsers(read(1).get(0), "127.0.0.1", "alice");

            client.addAuthInfo("digest", ("bob:" + SECRET).getBytes(StandardCharsets.UTF_8));
            client.exists("/", false);
            assertBindings(read(2), new String[][]{{"digest", "alice"}, {"digest", "bob"}});
            client.setData("/before-auth", new byte[]{2}, -1);
            assertUsers(read(1).get(0), "127.0.0.1", "alice", "bob");
            assertEquals("127.0.0.1", fields(before).get("user"));
            assertArrayEquals(new byte[]{2}, client.getData("/before-auth", false, null));
        } finally {
            fixture.tearDown();
        }
    }

    @Test
    public void testRealAuthFailurePreservesTheClientError() throws Exception {
        Standalone fixture = new Standalone();
        fixture.setUp();
        try {
            ZooKeeper client = fixture.connect();
            client.exists("/", false);
            String session = "0x" + Long.toHexString(client.getSessionId());
            read(1);
            client.addAuthInfo("missing", TOKEN.getBytes(StandardCharsets.UTF_8));
            try {
                client.exists("/", false);
                fail("Unknown authentication provider must still fail the client");
            } catch (KeeperException.AuthFailedException expected) {
                assertEquals(Code.AUTHFAILED, expected.code());
            }
            String log = read(1).get(0);
            assertBindingEvent(log, "authentication", session, "failure", "-115", "failed", "-4");
            assertBindings(log, new String[][]{{"missing", null}});
        } finally {
            fixture.tearDown();
        }
    }

    @Test
    public void testRealQuorumMovementRebindsTheSameSessionWithoutOldConnectionAuth() throws Exception {
        QuorumUtil quorum = new QuorumUtil(1);
        TestableZooKeeper original = null;
        ZooKeeper reconnected = null;
        try {
            quorum.startAll();
            capture.clear();
            ClientBase.CountdownWatcher watcher = new ClientBase.CountdownWatcher();
            original = new TestableZooKeeper(
                    quorum.getConnectString(quorum.getLeaderQuorumPeer()), ClientBase.CONNECTION_TIMEOUT, watcher);
            watcher.waitForConnected(ClientBase.CONNECTION_TIMEOUT);
            original.exists("/", false);
            long session = original.getSessionId();
            byte[] passwd = original.getSessionPasswd();
            String sessionText = "0x" + Long.toHexString(session);
            assertBindingEvent(read(1).get(0), "sessionEstablished", sessionText, "success", null, "unknown", null);
            original.addAuthInfo("digest", ("alice:" + SECRET).getBytes(StandardCharsets.UTF_8));
            original.create("/moving", new byte[]{1}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            List<String> before = read(2);
            assertBindings(before.get(0), new String[][]{{"digest", "alice"}});
            original.disconnect();

            watcher = new ClientBase.CountdownWatcher();
            reconnected = new ZooKeeper(
                    quorum.getConnectString(quorum.getFollowerQuorumPeers().get(0)),
                    ClientBase.CONNECTION_TIMEOUT, watcher, session, passwd);
            watcher.waitForConnected(ClientBase.CONNECTION_TIMEOUT);
            assertEquals(session, reconnected.getSessionId());
            assertArrayEquals(new byte[]{1}, reconnected.getData("/moving", false, null));
            String attachment = read(1).get(0);
            assertBindingEvent(attachment, "sessionEstablished", sessionText, "success", null, "unknown", null);
            assertBindings(attachment, new String[][]{{"ip", "127.0.0.1"}});
            reconnected.addAuthInfo("digest", ("bob:" + SECRET).getBytes(StandardCharsets.UTF_8));
            reconnected.setData("/moving", new byte[]{2}, -1);
            List<String> after = read(2);
            assertBindings(after.get(0), new String[][]{{"digest", "bob"}});
            assertUsers(after.get(1), "127.0.0.1", "bob");
            assertArrayEquals(new byte[]{2}, reconnected.getData("/moving", false, null));
            read(0);
        } finally {
            if (reconnected != null) {
                reconnected.close();
            }
            if (original != null) {
                original.close();
            }
            quorum.tearDown();
        }
    }

    @Test
    public void testFailingAuditSinkAndCounterDoNotRejectRealSessionAuthOrWrite() throws Exception {
        FailingCounter counter = new FailingCounter();
        Counter previousCounter = AuditHelperTest.replaceAuditErrorCounter(counter);
        Slf4jAuditLogger logger = new Slf4jAuditLogger();
        Object previousLogger = AuditHelperTest.replaceProviderField("auditLogger", (AuditLogger) event -> {
            logger.logAuditEvent(event);
            throw new IllegalStateException("synthetic C2 audit sink failure");
        });
        Standalone fixture = new Standalone();
        try {
            fixture.setUp();
            ZooKeeper client = fixture.connect();
            client.addAuthInfo("digest", ("alice:" + SECRET).getBytes(StandardCharsets.UTF_8));
            client.create("/audit-failure", new byte[]{9}, ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
            assertArrayEquals(new byte[]{9}, client.getData("/audit-failure", false, null));
            List<String> logs = read(3);
            assertEquals("sessionEstablished", fields(logs.get(0)).get("operation"));
            assertBindings(logs.get(1), new String[][]{{"digest", "alice"}});
            assertEquals("committed", fields(logs.get(2)).get("outcome"));
            assertEquals(3, counter.get());
        } finally {
            AuditHelperTest.replaceProviderField("auditLogger", previousLogger);
            AuditHelperTest.replaceAuditErrorCounter(previousCounter);
            fixture.tearDown();
        }
    }

    private void assertSaslFailure(long session, boolean allow, boolean require, Code replyCode, boolean closed)
            throws Exception {
        property(ZooKeeperServer.ALLOW_SASL_FAILED_CLIENTS, Boolean.toString(allow));
        property(ZooKeeperServer.SESSION_REQUIRE_CLIENT_SASL_AUTH, Boolean.toString(require));
        RecordingCnxn connection = new RecordingCnxn(session);
        try (SaslExchange sasl = new SaslExchange(connection, TOKEN)) {
            byte[] response = sasl.challenge();
            read(0);
            sasl.respond(response);
            assertFalse(sasl.server.isComplete());
            assertEquals(replyCode.intValue(), connection.lastReply().getErr());
            assertEquals(2, connection.replies.size());
            assertEquals(closed, connection.sessionClosed);
            String log = read(1).get(0);
            assertBindingEvent(log, "authentication", session == 0 ? null : "0x123",
                    "failure", "-115", "failed", "-33");
            assertBindings(log, new String[][]{{"sasl", null}});
        }
    }

    private void authenticate(RecordingCnxn connection, String scheme, String credentials) throws IOException {
        server.processPacket(connection, packet(OpCode.auth, AUTH_XID,
                new AuthPacket(0, scheme, credentials.getBytes(StandardCharsets.UTF_8))));
    }

    private static ByteBuffer packet(int type, int xid, Record record) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        BinaryOutputArchive archive = BinaryOutputArchive.getArchive(bytes);
        new RequestHeader(xid, type).serialize(archive, "header");
        record.serialize(archive, "request");
        return ByteBuffer.wrap(bytes.toByteArray());
    }

    private List<String> read(int count) {
        List<String> logs = capture.read(count);
        captured.addAll(logs);
        return logs;
    }

    private void property(String name, String value) {
        if (!previousProperties.containsKey(name)) {
            previousProperties.put(name, System.getProperty(name));
        }
        AuditHelperTest.restoreProperty(name, value);
    }

    private static Properties failEnhancedGateRead() {
        Properties previous = System.getProperties();
        Properties failing = new Properties() {
            @Override
            public String getProperty(String key) {
                if (AuditHelperTest.ENHANCED_ENABLE.equals(key)) {
                    throw new SecurityException("synthetic audit gate access failure");
                }
                return super.getProperty(key);
            }
        };
        failing.putAll(previous);
        System.setProperties(failing);
        return previous;
    }

    private static void assertBindingEvent(String log, String operation, String session, String result,
                                          String error, String outcome, String cxid) {
        Map<String, String> event = fields(log);
        assertEquals("2", event.get("schema_version"));
        assertEquals(operation, event.get("operation"));
        assertEquals(session, event.get("session"));
        assertEquals(result, event.get("result"));
        assertEquals(error, event.get("error_code"));
        assertEquals(outcome, event.get("outcome"));
        assertEquals(cxid, event.get("cxid"));
        assertEquals("127.0.0.1", event.get("ip"));
        assertNull(event.get("zxid"));
        assertNull(event.get("data_length"));
        assertNull(event.get("znode"));
    }

    private static void assertBindings(String log, String[][] expected) {
        assertBindings(Collections.singletonList(log), expected);
    }

    private static void assertBindings(List<String> logs, String[][] expected) {
        assertEquals(logs.toString(), expected.length, logs.size());
        Set<List<String>> actual = new HashSet<>();
        for (String log : logs) {
            Map<String, String> binding = fields(log);
            assertEquals("2", binding.get("schema_version"));
            actual.add(Arrays.asList(unescape(binding.get("auth_scheme")), unescape(binding.get("user"))));
        }
        assertEquals("Duplicate scheme/principal binding", logs.size(), actual.size());
        Set<List<String>> wanted = new HashSet<>();
        for (String[] binding : expected) {
            wanted.add(Arrays.asList(binding));
        }
        assertEquals(wanted, actual);
    }

    private static String unescape(String encoded) {
        if (encoded == null) {
            return null;
        }
        StringBuilder decoded = new StringBuilder();
        for (int i = 0; i < encoded.length(); i++) {
            char c = encoded.charAt(i);
            if (c == '\\') {
                char escaped = encoded.charAt(++i);
                switch (escaped) {
                    case '\\': c = '\\'; break;
                    case 't': c = '\t'; break;
                    case 'r': c = '\r'; break;
                    case 'n': c = '\n'; break;
                    default: fail("Unexpected v2 escape: " + escaped);
                }
            }
            decoded.append(c);
        }
        return decoded.toString();
    }

    private static void assertUsers(String log, String... users) {
        assertEquals(new HashSet<>(Arrays.asList(users)),
                new HashSet<>(Arrays.asList(fields(log).get("user").split(","))));
    }

    public static class EmptyAuthenticationProvider extends CredentialAuthenticationProvider {
        @Override
        public String getScheme() {
            return "audit-c2-empty";
        }

        @Override
        public Code handleAuthentication(ServerCnxn connection, byte[] data) {
            return Code.OK;
        }
    }

    private static final class Standalone extends ClientBase {
        private TestableZooKeeper connect() throws IOException, InterruptedException {
            return createClient();
        }
    }

    private static final class RecordingCnxn extends MockServerCnxn {
        private final List<ReplyHeader> replies = new ArrayList<>();
        private long sessionId;
        private boolean sessionClosed;
        private DisconnectReason disconnectReason;
        private SetSASLResponse saslResponse;
        private Runnable authInfoRead;

        private RecordingCnxn(long sessionId) {
            this.sessionId = sessionId;
        }

        @Override
        public long getSessionId() {
            return sessionId;
        }

        @Override
        public InetSocketAddress getRemoteSocketAddress() {
            return new InetSocketAddress("127.0.0.1", 2181);
        }

        @Override
        public List<Id> getAuthInfo() {
            if (authInfoRead != null) {
                authInfoRead.run();
            }
            return super.getAuthInfo();
        }

        @Override
        public void sendResponse(ReplyHeader header, Record record, String tag,
                                 String cacheKey, Stat stat, int opCode) {
            replies.add(header);
            if (record instanceof SetSASLResponse) {
                saslResponse = (SetSASLResponse) record;
            }
        }

        @Override
        public void sendCloseSession() {
            sessionClosed = true;
        }

        @Override
        public void close(DisconnectReason reason) {
            disconnectReason = reason;
        }

        private ReplyHeader lastReply() {
            assertFalse("Server must produce a protocol reply", replies.isEmpty());
            return replies.get(replies.size() - 1);
        }

        private void saslServer(ZooKeeperSaslServer saslServer) {
            zooKeeperSaslServer = saslServer;
        }
    }

    private final class SaslExchange implements AutoCloseable {
        private final SaslServer server;
        private final SaslClient client;
        private final RecordingCnxn connection;

        private SaslExchange(RecordingCnxn connection, String clientPassword) throws Exception {
            this.connection = connection;
            Map<String, String> properties = Collections.singletonMap(Sasl.QOP, "auth");
            server = Sasl.createSaslServer("DIGEST-MD5", "zookeeper", "localhost",
                    properties, callbacks(SECRET));
            client = Sasl.createSaslClient(new String[]{"DIGEST-MD5"}, null, "zookeeper", "localhost",
                    properties, callbacks(clientPassword));
            assertNotNull("JDK must provide DIGEST-MD5", server);
            assertNotNull("JDK must provide DIGEST-MD5", client);
            // The package-private wrapper constructor requires a JAAS Login; keep its methods and the JDK engine real.
            ZooKeeperSaslServer wrapper = mock(ZooKeeperSaslServer.class, CALLS_REAL_METHODS);
            Field engine = ZooKeeperSaslServer.class.getDeclaredField("saslServer");
            engine.setAccessible(true);
            engine.set(wrapper, server);
            connection.saslServer(wrapper);
        }

        private byte[] challenge() throws IOException {
            SessionAuthAuditTest.this.server.processPacket(connection,
                    packet(OpCode.sasl, SASL_XID, new GetSASLRequest(new byte[0])));
            assertEquals(Code.OK.intValue(), connection.lastReply().getErr());
            assertNotNull(connection.saslResponse.getToken());
            return client.evaluateChallenge(connection.saslResponse.getToken());
        }

        private void respond(byte[] response) throws IOException {
            SessionAuthAuditTest.this.server.processPacket(connection,
                    packet(OpCode.sasl, SASL_XID, new GetSASLRequest(response)));
            if (server.isComplete()) {
                client.evaluateChallenge(connection.saslResponse.getToken());
            }
        }

        @Override
        public void close() throws Exception {
            client.dispose();
            server.dispose();
        }
    }

    private static CallbackHandler callbacks(String password) {
        return callbacks -> {
            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback) {
                    ((NameCallback) callback).setName("alice");
                } else if (callback instanceof PasswordCallback) {
                    ((PasswordCallback) callback).setPassword(password.toCharArray());
                } else if (callback instanceof RealmCallback) {
                    RealmCallback realm = (RealmCallback) callback;
                    realm.setText(realm.getDefaultText());
                } else if (callback instanceof AuthorizeCallback) {
                    AuthorizeCallback authorization = (AuthorizeCallback) callback;
                    authorization.setAuthorized(
                            authorization.getAuthenticationID().equals(authorization.getAuthorizationID()));
                } else {
                    throw new UnsupportedCallbackException(callback);
                }
            }
        };
    }
}
