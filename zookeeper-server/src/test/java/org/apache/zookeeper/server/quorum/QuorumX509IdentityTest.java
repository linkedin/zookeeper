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

package org.apache.zookeeper.server.quorum;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.SpiffeAuthTestUtil;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.proto.SyncRequest;
import org.apache.zookeeper.server.MockServerCnxn;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.auth.X509AuthenticationUtil;
import org.apache.zookeeper.server.auth.X509AuthenticationUtil.CertificateType;
import org.apache.zookeeper.server.auth.X509AuthenticationUtil.ClientIdentity;
import org.apache.zookeeper.server.auth.X509QuorumAuthInfo;
import org.junit.BeforeClass;
import org.junit.Test;

public final class QuorumX509IdentityTest extends ZKTestCase {

    private static final long SESSION_ID = 0x123456789abcdefL;
    private static final int XID = 0x13579bdf;
    private static final String SPIFFE_V2_URI = "spiffe://example.org/v2/application/example-mp/kafka/cluster-a";
    private static final String SPIFFE_V2_ID = "application/example-mp/kafka/cluster-a";

    @BeforeClass
    public static void registerBouncyCastle() {
        SpiffeAuthTestUtil.registerBouncyCastle();
    }

    @Test
    public void testForwardedRequestPreservesTypedIdentityAndRequestBytes() throws Exception {
        MockServerCnxn cnxn = connectionWithIdentity(SPIFFE_V2_URI);
        ClientIdentity identity = cnxn.getX509ClientIdentity();
        assertEquals(CertificateType.SPIFFE_V2, identity.getCertificateType());
        assertEquals(SPIFFE_V2_ID, identity.getId());
        List<Id> originalAuth = copyIds(cnxn.getAuthInfo());
        byte[] body = setDataBody();
        Request request = new Request(cnxn, SESSION_ID, XID, OpCode.setData, ByteBuffer.wrap(body), cnxn.getAuthInfo());
        LearnerHandler handler = handler(mock(Leader.class));

        QuorumPacket packet = forward(request);
        List<Id> wireAuth = copyIds(packet.getAuthinfo());
        Request forwarded = handler.readRequest(packet);

        byte[] expectedPacketData = ByteBuffer.allocate(Long.BYTES + 2 * Integer.BYTES + body.length)
            .putLong(SESSION_ID).putInt(XID).putInt(OpCode.setData).put(body).array();
        assertArrayEquals(expectedPacketData, packet.getData());
        assertTransportAuthInfo(packet, originalAuth, identity);
        assertForwardedRequest(request, forwarded, body, identity);
        assertEquals(originalAuth, request.authInfo);
        assertEquals(originalAuth, cnxn.getAuthInfo());
        assertEquals(wireAuth, packet.getAuthinfo());
        assertArrayEquals(body, bufferBytes(request.request));
    }

    @Test
    public void testRelayPreservesIdentityWithExactlyOneTransportMarker() throws Exception {
        MockServerCnxn cnxn = connectionWithIdentity("spiffe://example.org/v1/application/example-mp/kafka");
        ClientIdentity identity = cnxn.getX509ClientIdentity();
        assertEquals(CertificateType.SPIFFE_V1_WORKLOAD, identity.getCertificateType());
        assertEquals("application/example-mp/kafka", identity.getId());
        List<Id> originalAuth = copyIds(cnxn.getAuthInfo());
        byte[] body = setDataBody();
        Request original = new Request(cnxn, SESSION_ID, XID, OpCode.setData, ByteBuffer.wrap(body), cnxn.getAuthInfo());
        LearnerHandler observerHandler = handler(mock(ObserverMaster.class));
        LearnerHandler leaderHandler = handler(mock(Leader.class));

        QuorumPacket firstPacket = forward(original);
        Request relayRequest = observerHandler.readRequest(firstPacket);
        QuorumPacket relayedPacket = forward(relayRequest);
        Request leaderRequest = leaderHandler.readRequest(relayedPacket);

        assertTransportAuthInfo(firstPacket, originalAuth, identity);
        assertTransportAuthInfo(relayedPacket, originalAuth, identity);
        assertForwardedRequest(original, relayRequest, body, identity);
        assertForwardedRequest(original, leaderRequest, body, identity);
        assertArrayEquals(firstPacket.getData(), relayedPacket.getData());
        assertEquals(originalAuth, original.authInfo);
        assertEquals(originalAuth, cnxn.getAuthInfo());
    }

    @Test
    public void testSyncRetainsIdentityAndReceivingHandlerAcrossRelay() throws Exception {
        MockServerCnxn cnxn = connectionWithIdentity("spiffe://example.org/v1/wl/kafka");
        ClientIdentity identity = cnxn.getX509ClientIdentity();
        assertEquals(CertificateType.SPIFFE_V1_WL, identity.getCertificateType());
        assertEquals("kafka", identity.getId());
        List<Id> originalAuth = copyIds(cnxn.getAuthInfo());
        byte[] body = serialize(new SyncRequest("/sync-target"));
        Request original = new Request(cnxn, SESSION_ID, XID, OpCode.sync, ByteBuffer.wrap(body), cnxn.getAuthInfo());
        LearnerHandler observerHandler = handler(mock(ObserverMaster.class));
        LearnerHandler leaderHandler = handler(mock(Leader.class));

        QuorumPacket firstPacket = forward(original);
        Request observerSync = observerHandler.readRequest(firstPacket);
        QuorumPacket relayedPacket = forward(observerSync);
        Request leaderSync = leaderHandler.readRequest(relayedPacket);

        assertTrue(observerSync instanceof LearnerSyncRequest);
        assertSame(observerHandler, ((LearnerSyncRequest) observerSync).fh);
        assertTrue(leaderSync instanceof LearnerSyncRequest);
        assertSame(leaderHandler, ((LearnerSyncRequest) leaderSync).fh);
        assertForwardedRequest(original, observerSync, body, identity);
        assertForwardedRequest(original, leaderSync, body, identity);
        assertTransportAuthInfo(firstPacket, originalAuth, identity);
        assertTransportAuthInfo(relayedPacket, originalAuth, identity);
        assertEquals(originalAuth, original.authInfo);
        assertEquals(originalAuth, cnxn.getAuthInfo());
    }

    @Test
    public void testRequestSnapshotsIdentityBeforeConnectionCacheIsCleared() throws Exception {
        MockServerCnxn cnxn = connectionWithIdentity(SPIFFE_V2_URI);
        ClientIdentity originalIdentity = cnxn.getX509ClientIdentity();
        List<Id> originalAuth = copyIds(cnxn.getAuthInfo());
        byte[] body = setDataBody();
        Request request = new Request(cnxn, SESSION_ID, XID, OpCode.setData, ByteBuffer.wrap(body), cnxn.getAuthInfo());

        cnxn.setX509ClientIdentity(null);
        QuorumPacket packet = forward(request);
        Request forwarded = handler(mock(Leader.class)).readRequest(packet);

        assertNull(cnxn.getX509ClientIdentity());
        assertSame(originalIdentity, request.getX509ClientIdentity());
        assertTransportAuthInfo(packet, originalAuth, originalIdentity);
        assertForwardedRequest(request, forwarded, body, originalIdentity);
        assertEquals(originalAuth, request.authInfo);
        assertEquals(originalAuth, cnxn.getAuthInfo());
    }

    @Test
    public void testRequestSnapshotsIdentityBeforeConnectionCacheIsReplaced() throws Exception {
        MockServerCnxn cnxn = connectionWithIdentity(SPIFFE_V2_URI);
        ClientIdentity originalIdentity = cnxn.getX509ClientIdentity();
        List<Id> originalAuth = copyIds(cnxn.getAuthInfo());
        byte[] body = setDataBody();
        Request request = new Request(cnxn, SESSION_ID, XID, OpCode.setData, ByteBuffer.wrap(body), cnxn.getAuthInfo());
        ClientIdentity replacement = connectionWithIdentity("spiffe://example.org/v1/wl/other-app")
            .getX509ClientIdentity();

        cnxn.setX509ClientIdentity(replacement);
        QuorumPacket packet = forward(request);
        Request forwarded = handler(mock(Leader.class)).readRequest(packet);

        assertSame(replacement, cnxn.getX509ClientIdentity());
        assertSame(originalIdentity, request.getX509ClientIdentity());
        assertTransportAuthInfo(packet, originalAuth, originalIdentity);
        assertForwardedRequest(request, forwarded, body, originalIdentity);
        assertEquals(originalAuth, request.authInfo);
        assertEquals(originalAuth, cnxn.getAuthInfo());
    }

    @Test
    public void testForwardingLegacyAuthWithoutMetadataDoesNotInferTypedIdentity() throws Exception {
        MockServerCnxn cnxn = new MockServerCnxn();
        cnxn.addAuthInfo(new Id("x509", "urn:li:servicePrincipal(kafka;region1;instance1)"));
        cnxn.addAuthInfo(new Id("x509", "1:SPIFFE_V2:application/not-metadata"));
        cnxn.addAuthInfo(new Id("ip", "127.0.0.1"));
        List<Id> originalAuth = copyIds(cnxn.getAuthInfo());
        byte[] body = setDataBody();
        Request original = new Request(cnxn, SESSION_ID, XID, OpCode.setData, ByteBuffer.wrap(body), cnxn.getAuthInfo());

        QuorumPacket packet = forward(original);
        Request forwarded = handler(mock(Leader.class)).readRequest(packet);
        QuorumPacket relayedPacket = forward(forwarded);
        Request relayed = handler(mock(Leader.class)).readRequest(relayedPacket);

        assertEquals(originalAuth, packet.getAuthinfo());
        assertEquals(originalAuth, relayedPacket.getAuthinfo());
        assertForwardedRequest(original, forwarded, body, null);
        assertForwardedRequest(original, relayed, body, null);
        assertNull(original.getX509ClientIdentity());
        assertEquals(originalAuth, cnxn.getAuthInfo());
    }

    @Test
    public void testForwardingNullOrEmptyAuthInfoAndBodyNeedsNoIdentity() throws Exception {
        for (List<Id> authInfo : Arrays.<List<Id>>asList(null, Collections.emptyList())) {
            Request request = new Request(null, SESSION_ID, XID, OpCode.closeSession, null, authInfo);

            QuorumPacket packet = forward(request);
            Request forwarded = handler(mock(Leader.class)).readRequest(packet);

            assertEquals(authInfo, packet.getAuthinfo());
            assertForwardedRequest(request, forwarded, new byte[0], null);
        }
    }

    @Test
    public void testReadRequestRejectsCorruptTransportMetadata() throws Exception {
        Request request = new Request(null, SESSION_ID, XID, OpCode.setData,
            ByteBuffer.wrap(setDataBody()), Collections.singletonList(new Id("ip", "127.0.0.1")));
        LearnerHandler handler = handler(mock(Leader.class));
        for (String invalid : Arrays.asList(null, "1:SPIFFE_V2", "2:SPIFFE_V2:kafka", "1:UNKNOWN:kafka")) {
            QuorumPacket packet = forward(request);
            packet.getAuthinfo().add(new Id(X509QuorumAuthInfo.AUTH_SCHEME, invalid));

            assertRequestRejected(handler, packet);
        }
    }

    @Test
    public void testReadRequestRejectsDuplicateAndConflictingTransportMetadata() throws Exception {
        MockServerCnxn cnxn = connectionWithIdentity(SPIFFE_V2_URI);
        List<Id> originalAuth = copyIds(cnxn.getAuthInfo());
        Request request = new Request(cnxn, SESSION_ID, XID, OpCode.setData,
            ByteBuffer.wrap(setDataBody()), cnxn.getAuthInfo());
        LearnerHandler handler = handler(mock(Leader.class));
        for (String duplicate : Arrays.asList(
            "1:SPIFFE_V2:" + SPIFFE_V2_ID, "1:LEGACY_SAN:urn:li:servicePrincipal(other;region;instance)")) {
            QuorumPacket packet = forward(request);
            packet.getAuthinfo().add(new Id(X509QuorumAuthInfo.AUTH_SCHEME, duplicate));

            assertRequestRejected(handler, packet);
            assertEquals(originalAuth, request.authInfo);
            assertEquals(originalAuth, cnxn.getAuthInfo());
        }
    }

    private static final class CapturingLearner extends Learner {
        private final List<QuorumPacket> packets = new ArrayList<>();
        private boolean flushed;

        @Override
        void writePacket(QuorumPacket packet, boolean flush) {
            packets.add(packet);
            flushed = flush;
        }
    }

    private static QuorumPacket forward(Request request) throws IOException {
        CapturingLearner learner = new CapturingLearner();
        learner.request(request);
        assertEquals(1, learner.packets.size());
        assertTrue(learner.flushed);
        QuorumPacket packet = learner.packets.get(0);
        assertEquals(Leader.REQUEST, packet.getType());
        assertEquals(-1L, packet.getZxid());

        // Exercise the actual Jute authinfo vector rather than passing an in-memory list directly.
        QuorumPacket received = new QuorumPacket();
        BinaryInputArchive.getArchive(new ByteArrayInputStream(serialize(packet))).readRecord(received, "packet");
        assertEquals(packet.getType(), received.getType());
        assertEquals(packet.getZxid(), received.getZxid());
        assertArrayEquals(packet.getData(), received.getData());
        assertEquals(packet.getAuthinfo(), received.getAuthinfo());
        return received;
    }

    private static LearnerHandler handler(LearnerMaster master) throws IOException {
        Socket socket = mock(Socket.class);
        when(socket.getRemoteSocketAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 12345));
        when(socket.getInputStream()).thenReturn(new ByteArrayInputStream(new byte[0]));
        return new LearnerHandler(socket, new BufferedInputStream(socket.getInputStream()), master);
    }

    private static MockServerCnxn connectionWithIdentity(String uri) throws Exception {
        X509Certificate certificate = SpiffeAuthTestUtil.buildClientCertWithUriSans(uri);
        ClientIdentity identity = X509AuthenticationUtil.getClientId(certificate);
        MockServerCnxn cnxn = new MockServerCnxn();
        cnxn.setX509ClientIdentity(identity);
        cnxn.addAuthInfo(new Id("ip", "127.0.0.1"));
        cnxn.addAuthInfo(new Id("x509", identity.getId()));
        cnxn.addAuthInfo(new Id("digest", "client:hashed-credentials"));
        return cnxn;
    }

    private static byte[] setDataBody() throws IOException {
        return serialize(new SetDataRequest("/protected:node", new byte[]{0, 1, -1, 127, -128}, 7));
    }

    private static byte[] serialize(Record record) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        BinaryOutputArchive.getArchive(bytes).writeRecord(record, "packet");
        return bytes.toByteArray();
    }

    private static void assertForwardedRequest(Request original, Request forwarded, byte[] body, ClientIdentity identity) {
        assertNull(forwarded.cnxn);
        assertEquals(original.sessionId, forwarded.sessionId);
        assertEquals(original.cxid, forwarded.cxid);
        assertEquals(original.type, forwarded.type);
        assertArrayEquals(body, bufferBytes(forwarded.request));
        assertEquals(original.authInfo, forwarded.authInfo);
        if (identity == null) {
            assertNull(forwarded.getX509ClientIdentity());
        } else {
            assertEquals(identity.getCertificateType(), forwarded.getX509ClientIdentity().getCertificateType());
            assertEquals(identity.getId(), forwarded.getX509ClientIdentity().getId());
        }
    }

    private static void assertTransportAuthInfo(QuorumPacket packet, List<Id> ordinary, ClientIdentity identity) {
        List<Id> expected = copyIds(ordinary);
        expected.add(new Id(X509QuorumAuthInfo.AUTH_SCHEME,
            "1:" + identity.getCertificateType().name() + ":" + identity.getId()));
        assertEquals(expected, packet.getAuthinfo());
    }

    private static void assertRequestRejected(LearnerHandler handler, QuorumPacket packet) {
        List<Id> expectedAuth = copyIds(packet.getAuthinfo());
        try {
            handler.readRequest(packet);
            fail("Malformed or duplicate transport metadata must reject the forwarded request");
        } catch (IOException expected) {
            // Generated Id.equals cannot compare the deliberately malformed null ID.
            assertEquals(expectedAuth.size(), packet.getAuthinfo().size());
            for (int i = 0; i < expectedAuth.size(); i++) {
                assertEquals(expectedAuth.get(i).getScheme(), packet.getAuthinfo().get(i).getScheme());
                assertEquals(expectedAuth.get(i).getId(), packet.getAuthinfo().get(i).getId());
            }
        }
    }

    private static byte[] bufferBytes(ByteBuffer buffer) {
        ByteBuffer copy = buffer.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }

    private static List<Id> copyIds(List<Id> ids) {
        List<Id> copy = new ArrayList<>();
        for (Id id : ids) {
            copy.add(new Id(id.getScheme(), id.getId()));
        }
        return copy;
    }
}
