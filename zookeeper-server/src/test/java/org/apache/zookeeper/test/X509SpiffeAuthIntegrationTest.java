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

import static org.junit.Assert.assertEquals;
import java.security.cert.X509Certificate;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.common.SpiffeAuthTestUtil;
import org.apache.zookeeper.server.MockServerCnxn;
import org.apache.zookeeper.server.auth.X509AuthenticationProvider;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration tests for SPIFFE SAN-based client identity extraction. Unlike
 * {@link X509AuthTest}, which uses a hand-rolled mock cert, these tests construct REAL
 * {@link X509Certificate} instances (BouncyCastle-signed) with SPIFFE URI SANs and run them
 * through the full {@link X509AuthenticationProvider#handleAuthentication} path. This exercises
 * the JDK's actual SAN parsing, which the mock cert bypasses.
 */
public class X509SpiffeAuthIntegrationTest extends ZKTestCase {

    @BeforeClass
    public static void registerBouncyCastle() {
        SpiffeAuthTestUtil.registerBouncyCastle();
    }

    @After
    public void tearDown() {
        SpiffeAuthTestUtil.clearSpiffeSystemProperties();
    }

    @Test
    public void testRealCertWithSpiffeV1UriSanFallsBackToSubjectDn() throws Exception {
        SpiffeAuthTestUtil.setSpiffeSystemProperties();
        X509Certificate cert = SpiffeAuthTestUtil.buildClientCertWithUriSans(
                "spiffe://prod.lipki/v1/wl/espresso-router");

        String id = runAuth(cert);

        assertEquals(cert.getSubjectX500Principal().getName(), id);
    }

    @Test
    public void testRealCertWithSpiffeV2UriSanIsExtracted() throws Exception {
        SpiffeAuthTestUtil.setSpiffeSystemProperties();
        X509Certificate cert = SpiffeAuthTestUtil.buildClientCertWithUriSans(
                "spiffe://prod.lipki/v2/application/espresso-router/espresso-router");

        String id = runAuth(cert);

        assertEquals("application/espresso-router/espresso-router", id);
    }

    @Test
    public void testRealCertWithSpiffeUserUriFallsBackToSubjectDn() throws Exception {
        SpiffeAuthTestUtil.setSpiffeSystemProperties();
        X509Certificate cert = SpiffeAuthTestUtil.buildClientCertWithUriSans(
                "spiffe://prod.lipki/v2/user/alice");

        String id = runAuth(cert);

        // User identity is rejected for service-principal extraction; falls through to URN
        // (not configured here) and then to Subject DN.
        assertEquals(cert.getSubjectX500Principal().getName(), id);
    }

    @Test
    public void testRealCertWithoutSpiffeSanFallsBackToSubjectDn() throws Exception {
        SpiffeAuthTestUtil.setSpiffeSystemProperties();
        // URI SAN that is not a SPIFFE URI — should not match the SPIFFE regex.
        X509Certificate cert = SpiffeAuthTestUtil.buildClientCertWithUriSans(
                "urn:li:servicePrincipal(legacy-app;ei4;i001)");

        String id = runAuth(cert);

        // No SPIFFE match, URN extraction not configured here, so falls to Subject DN.
        assertEquals(cert.getSubjectX500Principal().getName(), id);
    }

    /**
     * Defense-in-depth: a SAN whose single literal path segment contains {@code %2F} must not
     * be silently promoted to a multi-segment principal via URI decoding (which could collide
     * with an unrelated legitimate identity). Falls through to Subject DN.
     */
    @Test
    public void testRealCertWithPercentEncodedSlashInPathFallsBackToSubjectDn() throws Exception {
        SpiffeAuthTestUtil.setSpiffeSystemProperties();
        X509Certificate cert = SpiffeAuthTestUtil.buildClientCertWithUriSans(
                "spiffe://prod.lipki/v2/application%2Ffoo-mp%2Fbar-app");

        String id = runAuth(cert);

        assertEquals(cert.getSubjectX500Principal().getName(), id);
    }

    /**
     * Defense-in-depth: a percent-encoded "user" segment ({@code %75ser}) must not bypass the
     * user-identity rejection. Falls through to Subject DN.
     */
    @Test
    public void testRealCertWithPercentEncodedUserPathFallsBackToSubjectDn() throws Exception {
        SpiffeAuthTestUtil.setSpiffeSystemProperties();
        X509Certificate cert = SpiffeAuthTestUtil.buildClientCertWithUriSans(
                "spiffe://prod.lipki/v2/%75ser/alice");

        String id = runAuth(cert);

        assertEquals(cert.getSubjectX500Principal().getName(), id);
    }

    private static String runAuth(X509Certificate cert) {
        X509AuthenticationProvider provider = new X509AuthenticationProvider(
                new SpiffeAuthTestUtil.AcceptAllTrustManager(),
                new SpiffeAuthTestUtil.NoopKeyManager());
        MockServerCnxn cnxn = new MockServerCnxn();
        cnxn.clientChain = new X509Certificate[]{cert};
        assertEquals(KeeperException.Code.OK, provider.handleAuthentication(cnxn, null));
        return cnxn.getAuthInfo().get(0).getId();
    }
}
