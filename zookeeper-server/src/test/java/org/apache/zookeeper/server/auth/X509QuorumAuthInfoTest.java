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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.PrepRequestProcessor;
import org.apache.zookeeper.server.auth.X509AuthenticationUtil.CertificateType;
import org.apache.zookeeper.server.auth.X509AuthenticationUtil.ClientIdentity;
import org.junit.Test;

public final class X509QuorumAuthInfoTest extends ZKTestCase {

    private static final String ORIGINAL_ID = "application/example-mp/Kafka:blue;instance";
    private static final String PROVIDER_PROPERTY =
        ProviderRegistry.AUTHPROVIDER_PROPERTY_PREFIX + "quorum-metadata-regression";

    @Test
    public void testRoundTripEveryCertificateTypePreservesOriginalIds() throws Exception {
        for (CertificateType type : CertificateType.values()) {
            for (String originalId : Arrays.asList(
                "Kafka",
                ORIGINAL_ID,
                "servicePrincipal(kafka",
                "urn:li:servicePrincipal(kafka;region1;instance1)",
                "CN=Kafka:blue;instance,O=Example")) {
                assertRoundTrip(type, originalId, ordinaryAuthInfo());
            }
        }
    }

    @Test
    public void testRoundTripPreservesEmptyLegacyId() throws Exception {
        assertRoundTrip(CertificateType.LEGACY_SAN, "", ordinaryAuthInfo());
    }

    @Test
    public void testRoundTripIdentityWithoutOrdinaryAuthInfo() throws Exception {
        assertRoundTrip(CertificateType.SPIFFE_V2, ORIGINAL_ID, null);
        assertRoundTrip(CertificateType.SPIFFE_V2, ORIGINAL_ID, Collections.emptyList());
    }

    @Test
    public void testAbsentMetadataPreservesNullEmptyAndOrdinaryAuthInfo() throws Exception {
        assertNull(X509QuorumAuthInfo.encode(null, null));
        X509QuorumAuthInfo nullAuth = X509QuorumAuthInfo.decode(null);
        assertNull(nullAuth.getAuthInfo());
        assertNull(nullAuth.getClientIdentity());

        for (List<Id> authInfo : Arrays.asList(Collections.<Id>emptyList(), ordinaryAuthInfo())) {
            List<Id> expected = copyIds(authInfo);
            List<Id> encoded = X509QuorumAuthInfo.encode(Collections.unmodifiableList(authInfo), null);
            X509QuorumAuthInfo decoded = X509QuorumAuthInfo.decode(encoded);

            assertEquals(expected, encoded);
            assertEquals(expected, decoded.getAuthInfo());
            assertEquals(expected, authInfo);
            assertNull(decoded.getClientIdentity());
        }
    }

    @Test
    public void testDecodeStripsMetadataAtAnyPositionWithoutChangingWireList() throws Exception {
        List<Id> ordinary = ordinaryAuthInfo();
        for (int position = 0; position <= ordinary.size(); position++) {
            List<Id> wire = copyIds(ordinary);
            wire.add(position, marker("1:SPIFFE_V2:" + ORIGINAL_ID));
            List<Id> expectedWire = copyIds(wire);

            X509QuorumAuthInfo decoded = X509QuorumAuthInfo.decode(Collections.unmodifiableList(wire));

            assertEquals(ordinary, decoded.getAuthInfo());
            assertEquals(CertificateType.SPIFFE_V2, decoded.getClientIdentity().getCertificateType());
            assertEquals(ORIGINAL_ID, decoded.getClientIdentity().getId());
            assertEquals(expectedWire, wire);
        }
    }

    @Test
    public void testDecodeRejectsMalformedOrUnsupportedMetadata() {
        for (String value : Arrays.asList(
            null, "", "1", "1:SPIFFE_V2", ":SPIFFE_V2:kafka",
            "0:SPIFFE_V2:kafka", "2:SPIFFE_V2:kafka", "01:SPIFFE_V2:kafka",
            "1::kafka", "1:UNKNOWN:kafka", "1:spiffe_v2:kafka")) {
            assertDecodeRejected(Arrays.asList(new Id("ip", "127.0.0.1"), marker(value)));
        }
    }

    @Test
    public void testDecodeRejectsDuplicateAndConflictingMarkers() {
        Id first = marker("1:SPIFFE_V2:" + ORIGINAL_ID);
        for (Id second : Arrays.asList(
            marker(first.getId()),
            marker("1:SPIFFE_V1_WORKLOAD:" + ORIGINAL_ID),
            marker("1:SPIFFE_V2:application/other-mp/other-app"))) {
            assertDecodeRejected(Arrays.asList(first, new Id("ip", "127.0.0.1"), second));
        }
        assertDecodeRejected(Arrays.asList(marker("1:LEGACY_SAN:"), marker("1:LEGACY_SAN:")));
    }

    @Test
    public void testEncodeRejectsContaminatedAuthInfoWithOrWithoutIdentity() {
        ClientIdentity identity = new ClientIdentity(CertificateType.SPIFFE_V2, ORIGINAL_ID);
        for (ClientIdentity context : Arrays.asList(null, identity)) {
            for (String value : Arrays.asList(null, "invalid", "1:SPIFFE_V2:" + ORIGINAL_ID)) {
                List<Id> authInfo = ordinaryAuthInfo();
                authInfo.add(1, marker(value));
                List<Id> expected = copyIds(authInfo);

                try {
                    X509QuorumAuthInfo.encode(Collections.unmodifiableList(authInfo), context);
                    fail("Reserved metadata must not be accepted as ordinary AuthInfo");
                } catch (IOException expectedException) {
                    assertIdsEqual(expected, authInfo);
                }
            }
        }
    }

    @Test
    public void testReservedSchemeCannotBeUsedForClientAuthOrExplicitAclEvenWhenRegistered() throws Exception {
        String originalProvider = System.getProperty(PROVIDER_PROPERTY);
        try {
            System.setProperty(PROVIDER_PROPERTY, ReservedSchemeProvider.class.getName());
            ProviderRegistry.reset();
            ProviderRegistry.initialize();
            assertTrue(ProviderRegistry.listProviders().contains(X509QuorumAuthInfo.AUTH_SCHEME + " "));

            assertNull(ProviderRegistry.getProvider(X509QuorumAuthInfo.AUTH_SCHEME));
            assertNull(ProviderRegistry.getServerProvider(X509QuorumAuthInfo.AUTH_SCHEME));
            List<ACL> acls = Collections.singletonList(
                new ACL(ZooDefs.Perms.ALL, marker("1:SPIFFE_V2:" + ORIGINAL_ID)));
            try {
                PrepRequestProcessor.fixupACL("/protected", Collections.emptyList(), acls);
                fail("Quorum metadata must not be accepted as an explicit ACL");
            } catch (KeeperException.InvalidACLException expected) {
                assertEquals(KeeperException.Code.INVALIDACL, expected.code());
            }
        } finally {
            if (originalProvider == null) {
                System.clearProperty(PROVIDER_PROPERTY);
            } else {
                System.setProperty(PROVIDER_PROPERTY, originalProvider);
            }
            ProviderRegistry.reset();
        }
    }

    public static final class ReservedSchemeProvider extends IPAuthenticationProvider {
        @Override
        public String getScheme() {
            return X509QuorumAuthInfo.AUTH_SCHEME;
        }

        @Override
        public boolean isValid(String id) {
            return true;
        }
    }

    private static void assertRoundTrip(CertificateType type, String originalId, List<Id> ordinary)
        throws IOException {
        List<Id> expectedOrdinary = ordinary == null ? Collections.emptyList() : copyIds(ordinary);
        List<Id> expectedWire = copyIds(expectedOrdinary);
        expectedWire.add(marker("1:" + type.name() + ":" + originalId));
        List<Id> input = ordinary == null ? null : Collections.unmodifiableList(ordinary);

        List<Id> encoded = X509QuorumAuthInfo.encode(input, new ClientIdentity(type, originalId));
        assertEquals(expectedWire, encoded);
        X509QuorumAuthInfo decoded = X509QuorumAuthInfo.decode(Collections.unmodifiableList(encoded));

        assertEquals(type, decoded.getClientIdentity().getCertificateType());
        assertEquals(originalId, decoded.getClientIdentity().getId());
        assertEquals(expectedOrdinary, decoded.getAuthInfo());
        assertEquals(expectedWire, encoded);
        if (ordinary != null) {
            assertEquals(expectedOrdinary, ordinary);
        }
    }

    private static void assertDecodeRejected(List<Id> wire) {
        List<Id> expectedWire = copyIds(wire);
        try {
            X509QuorumAuthInfo.decode(Collections.unmodifiableList(wire));
            fail("Malformed or duplicate quorum metadata must be rejected");
        } catch (IOException expected) {
            assertIdsEqual(expectedWire, wire);
        }
    }

    private static void assertIdsEqual(List<Id> expected, List<Id> actual) {
        // Generated Id.equals dereferences the ID, including deliberately malformed null IDs.
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i).getScheme(), actual.get(i).getScheme());
            assertEquals(expected.get(i).getId(), actual.get(i).getId());
        }
    }

    private static List<Id> ordinaryAuthInfo() {
        return new ArrayList<>(Arrays.asList(
            new Id("x509", "urn:li:servicePrincipal(kafka;region1;instance1)"),
            new Id("ip", "127.0.0.1"),
            new Id("x509", "1:SPIFFE_V2:application/not-metadata"),
            new Id("digest", "client:hashed-credentials")));
    }

    private static Id marker(String value) {
        return new Id(X509QuorumAuthInfo.AUTH_SCHEME, value);
    }

    private static List<Id> copyIds(List<Id> ids) {
        List<Id> copy = new ArrayList<>();
        for (Id id : ids) {
            copy.add(new Id(id.getScheme(), id.getId()));
        }
        return copy;
    }
}
