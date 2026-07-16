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

package org.apache.zookeeper.common;

import java.net.Socket;
import java.security.KeyPair;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.Security;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;
import org.apache.zookeeper.server.auth.X509AuthenticationConfig;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

/**
 * Test fixtures for SPIFFE-based authentication: BouncyCastle bootstrap, system-property
 * setup/teardown for SAN-based extraction, real X509 client cert builder with URI SANs, and stub
 * TLS managers. Shared across SPIFFE auth tests.
 *
 * <p>Note: SPIFFE identity extraction itself is always active (not gated behind any config), so
 * {@link #setSpiffeSystemProperties()} only needs to configure {@code clientCertIdType=SAN},
 * which some tests in this suite also exercise for legacy URN fallback behavior.
 */
public final class SpiffeAuthTestUtil {

  public static final long ONE_DAY_MILLIS = 24L * 60 * 60 * 1000;

  private SpiffeAuthTestUtil() {
  }

  public static void registerBouncyCastle() {
    if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
      Security.addProvider(new BouncyCastleProvider());
    }
  }

  /** Configures SAN-based extraction in the X509AuthenticationConfig singleton. */
  public static void setSpiffeSystemProperties() {
    System.setProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_TYPE, "SAN");
    X509AuthenticationConfig.reset();
  }

  public static void clearSpiffeSystemProperties() {
    System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_TYPE);
    X509AuthenticationConfig.reset();
  }

  /**
   * Builds a real BouncyCastle-signed X509 client certificate with the given URI SANs. Requires
   * {@link #registerBouncyCastle()} to have been called once per JVM.
   */
  public static X509Certificate buildClientCertWithUriSans(String... uriSans) throws Exception {
    KeyPair caKey = X509TestHelpers.generateRSAKeyPair();
    X509Certificate caCert = X509TestHelpers.newSelfSignedCACert(
        new X500Name("CN=Test CA"), caKey, ONE_DAY_MILLIS);
    KeyPair clientKey = X509TestHelpers.generateRSAKeyPair();
    GeneralName[] names = new GeneralName[uriSans.length];
    for (int i = 0; i < uriSans.length; i++) {
      names[i] = new GeneralName(GeneralName.uniformResourceIdentifier, uriSans[i]);
    }
    return X509TestHelpers.newCertWithSans(caCert, caKey,
        new X500Name("CN=test-client"), clientKey.getPublic(),
        new GeneralNames(names), ONE_DAY_MILLIS);
  }

  /** Trust manager that accepts any cert; for auth-flow tests that don't exercise trust validation. */
  public static final class AcceptAllTrustManager implements X509TrustManager {
    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
    }
    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
    }
    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return new X509Certificate[0];
    }
  }

  /** Key manager that returns null for everything; for tests that don't serve outbound TLS. */
  public static final class NoopKeyManager implements X509KeyManager {
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
}
