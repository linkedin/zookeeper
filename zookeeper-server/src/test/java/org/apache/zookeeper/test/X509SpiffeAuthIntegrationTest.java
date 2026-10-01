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
import static org.junit.Assert.assertNull;
import java.security.cert.X509Certificate;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.common.SpiffeAuthTestUtil;
import org.apache.zookeeper.server.MockServerCnxn;
import org.apache.zookeeper.server.auth.X509AuthenticationConfig;
import org.apache.zookeeper.server.auth.X509AuthenticationProvider;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration tests for SPIFFE SAN-based client identity extraction. This is the authoritative
 * test suite for SPIFFE certificate validation and principal extraction: every test constructs a
 * REAL {@link X509Certificate} (BouncyCastle-signed) with real SPIFFE URI SANs and runs it
 * through the full {@link X509AuthenticationProvider#handleAuthentication} path, exercising the
 * JDK's actual ASN.1/SAN parsing end-to-end rather than a hand-rolled mock. {@link X509AuthTest}
 * retains only the generic (non-SPIFFE) auth/SAN-regex tests, which still use a lightweight fake
 * {@code X509Certificate} since they don't need real certificate parsing.
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
    public void testRealCertWithSpiffeV1WlUriSanIsExtracted() throws Exception {
        SpiffeAuthTestUtil.setSpiffeSystemProperties();
        // v1 workload path "/v1/wl/<app-name>"; the "wl/" type prefix is stripped, principal is
        // just the app-name.
        X509Certificate cert = SpiffeAuthTestUtil.buildClientCertWithUriSans(
                "spiffe://prod.lipki/v1/wl/espresso-router");

        String id = runAuth(cert);

        assertEquals("espresso-router", id);
    }

    @Test
    public void testRealCertWithSpiffeV1WlUriSanIsExtractedWithoutAnyConfiguration() throws Exception {
        // Same "not a feature flag" guarantee as the v2 case, for the v1/wl form: zero system
        // properties set, real BouncyCastle-signed cert.
        assertNull("Test must start with no clientCertIdType configured",
                System.getProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_TYPE));
        X509Certificate cert = SpiffeAuthTestUtil.buildClientCertWithUriSans(
                "spiffe://prod.lipki/v1/wl/espresso-router");

        String id = runAuth(cert);

        assertEquals("espresso-router", id);
    }

    @Test
    public void testRealCertWithSpiffeV1WlMultiSegmentFallsBackToSubjectDn() throws Exception {
        // v1/wl app-name must be a single path segment. A multi-segment value after "wl/" (e.g.
        // "a/b") must NOT be accepted as a single app-name principal; falls through to Subject
        // DN (URN extraction not configured here).
        SpiffeAuthTestUtil.setSpiffeSystemProperties();
        X509Certificate cert = SpiffeAuthTestUtil.buildClientCertWithUriSans(
                "spiffe://prod.lipki/v1/wl/a/b");

        String id = runAuth(cert);

        assertEquals(cert.getSubjectX500Principal().getName(), id);
    }

    @Test
    public void testRealCertWithSpiffeV1UserIdentityRejectedFallsBackToSubjectDn() throws Exception {
        // v1 user-identity is rejected for two independent reasons: user-identity rejection AND
        // v1 "user" not being a recognized workload sub-type. Falls through to Subject DN.
        SpiffeAuthTestUtil.setSpiffeSystemProperties();
        X509Certificate cert = SpiffeAuthTestUtil.buildClientCertWithUriSans(
                "spiffe://prod.lipki/v1/user/alice");

        String id = runAuth(cert);

        assertEquals(cert.getSubjectX500Principal().getName(), id);
    }

    @Test
    public void testRealCertWithSpiffeV1ApplicationUriSanIsExtracted() throws Exception {
        SpiffeAuthTestUtil.setSpiffeSystemProperties();
        // LISPIFFE-ID spec §2.A: v1 also supports "application/<...>"; unlike "wl/", the type
        // prefix is retained in the extracted principal.
        X509Certificate cert = SpiffeAuthTestUtil.buildClientCertWithUriSans(
                "spiffe://prod.lipki/v1/application/foo-mp/bar-app");

        String id = runAuth(cert);

        assertEquals("application/foo-mp/bar-app", id);
    }

    @Test
    public void testRealCertWithSpiffeV1AirflowUriSanIsExtracted() throws Exception {
        SpiffeAuthTestUtil.setSpiffeSystemProperties();
        // LISPIFFE-ID spec §2.A: v1 also supports "airflow/<....>" for Airflow DAG workloads.
        X509Certificate cert = SpiffeAuthTestUtil.buildClientCertWithUriSans(
                "spiffe://prod.lipki/v1/airflow/my-dag");

        String id = runAuth(cert);

        assertEquals("airflow/my-dag", id);
    }

    @Test
    public void testRealCertWithSpiffeV1WorkflowUriSanFallsBackToSubjectDn() throws Exception {
        SpiffeAuthTestUtil.setSpiffeSystemProperties();
        // v1 Flyte workflow ("wf/") remains out of scope for ZK and must fall through, even
        // though "application/" and "airflow/" are now recognized.
        X509Certificate cert = SpiffeAuthTestUtil.buildClientCertWithUriSans(
                "spiffe://prod.lipki/v1/wf/some-workflow");

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
    public void testRealCertWithSpiffeV2WorkloadUriSanIsExtracted() throws Exception {
        // LISPIFFE-ID spec §2.B: v2 also supports the "workload/" sub-type, retaining the full
        // path (like "application/") rather than stripping the prefix (like v1's "wl/").
        SpiffeAuthTestUtil.setSpiffeSystemProperties();
        X509Certificate cert = SpiffeAuthTestUtil.buildClientCertWithUriSans(
                "spiffe://prod.lipki/v2/workload/foo-mp/bar-app/some-tag");

        String id = runAuth(cert);

        assertEquals("workload/foo-mp/bar-app/some-tag", id);
    }

    @Test
    public void testRealCertWithSpiffeV2UriSanIsExtractedWithoutAnyConfiguration() throws Exception {
        // Core "not a feature flag" guarantee, exercised against a real BouncyCastle-signed
        // cert (not the hand-rolled mock in X509AuthTest): SPIFFE detection must succeed even
        // with zero system properties set — not even clientCertIdType=SAN.
        assertNull("Test must start with no clientCertIdType configured",
                System.getProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_TYPE));
        X509Certificate cert = SpiffeAuthTestUtil.buildClientCertWithUriSans(
                "spiffe://prod.lipki/v2/application/espresso-router/espresso-router");

        String id = runAuth(cert);

        assertEquals("application/espresso-router/espresso-router", id);
    }

    @Test
    public void testRealCertWithSpiffeV2UserIdentityRejectedWithoutAnyConfiguration() throws Exception {
        // Rejection of user identities must also hold with zero configuration — a human's
        // SPIFFE cert (real, BouncyCastle-signed) must never be promoted to a service principal,
        // feature flag or not.
        assertNull("Test must start with no clientCertIdType configured",
                System.getProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_TYPE));
        X509Certificate cert = SpiffeAuthTestUtil.buildClientCertWithUriSans(
                "spiffe://prod.lipki/v2/user/alice");

        String id = runAuth(cert);

        assertEquals(cert.getSubjectX500Principal().getName(), id);
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

    @Test
    public void testRealCertWithNonSpiffeSanFallsBackToUrnWhenUrnConfigured() throws Exception {
        // Cert has a URN SAN (not a spiffe:// URI). The always-on SPIFFE check finds no match,
        // so it falls through to legacy URN-based SAN extraction (which IS configured here,
        // unlike testRealCertWithoutSpiffeSanFallsBackToSubjectDn above).
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_TYPE, "SAN");
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_TYPE, "6");
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_REGEX, "^.*urn:li:.*$");
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_REGEX,
                "^.*urn:li:([a-z]+Principal\\([^;%:]+)");
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_MATCHER_GROUP_INDEX, "1");
        try {
            X509Certificate cert = SpiffeAuthTestUtil.buildClientCertWithUriSans(
                    "urn:li:servicePrincipal(espresso-router;ei4;i001)");

            String id = runAuth(cert);

            assertEquals("servicePrincipal(espresso-router", id);
        } finally {
            System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_TYPE);
            System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_REGEX);
            System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_REGEX);
            System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_MATCHER_GROUP_INDEX);
        }
    }

    @Test
    public void testRealCertWithMultipleSpiffeSansFallsBackToSubjectDn() throws Exception {
        // Cert with >1 SPIFFE SAN — extractor throws, caught in getClientId, falls through to
        // URN (not configured) then to Subject DN.
        SpiffeAuthTestUtil.setSpiffeSystemProperties();
        X509Certificate cert = SpiffeAuthTestUtil.buildClientCertWithUriSans(
                "spiffe://prod.lipki/v2/application/espresso-router/espresso-router",
                "spiffe://prod.lipki/v2/application/another-service/another-service");

        String id = runAuth(cert);

        assertEquals(cert.getSubjectX500Principal().getName(), id);
    }

    @Test
    public void testRealCertPrefersSpiffeOverUrnWhenBothPresent() throws Exception {
        // Cert has BOTH a URN-format SAN and a SPIFFE SAN. SPIFFE must win, even though URN
        // extraction is also configured and would otherwise match.
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_TYPE, "SAN");
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_TYPE, "6");
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_REGEX, "^.*urn:li:.*$");
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_REGEX,
                "^.*urn:li:([a-z]+Principal\\([^;%:]+)");
        System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_MATCHER_GROUP_INDEX, "1");
        try {
            X509Certificate cert = SpiffeAuthTestUtil.buildClientCertWithUriSans(
                    "urn:li:servicePrincipal(legacy-app;ei4;i001)",
                    "spiffe://prod.lipki/v2/application/espresso-router/espresso-router");

            String id = runAuth(cert);

            // SPIFFE wins — path-after-/v2/, not the URN-derived legacy-app id.
            assertEquals("application/espresso-router/espresso-router", id);
        } finally {
            System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_TYPE);
            System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_REGEX);
            System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_REGEX);
            System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_MATCHER_GROUP_INDEX);
        }
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
