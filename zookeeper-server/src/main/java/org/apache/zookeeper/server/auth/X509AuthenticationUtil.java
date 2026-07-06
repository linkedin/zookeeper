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

import java.net.URI;
import java.security.cert.CertificateException;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.common.ClientX509Util;
import org.apache.zookeeper.common.X509Exception;
import org.apache.zookeeper.common.X509Util;
import org.apache.zookeeper.common.ZKConfig;
import org.apache.zookeeper.server.ServerCnxn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Util class for x509 certificate-based authentication providers.
 */
public class X509AuthenticationUtil extends X509Util {

  private static final Logger LOG = LoggerFactory.getLogger(X509AuthenticationUtil.class);

  // Super user Auth Id scheme
  public static final String SUPERUSER_AUTH_SCHEME = "super";
  public static final String X509_SCHEME = "x509";

  // Matches any SPIFFE URI, regardless of trust domain or version. SPIFFE detection is always
  // active — not gated behind operator config — and relies entirely on the TLS trust manager to
  // reject certificates from untrusted issuers before this code is ever reached.
  private static final Pattern SPIFFE_URI_PATTERN = Pattern.compile("^spiffe://.*$");

  // Matches LISPIFFE user-identity paths of the form "/v<N>/user" or "/v<N>/user/<rest>".
  // User-identity SPIFFE certs (issued to humans, not workloads) must NOT be promoted to a
  // service principal, otherwise a user credential would be granted service-level ACL access.
  // See LISPIFFE-ID spec: https://github.com/linkedin-multiproduct/gopki/blob/master/LISPIFFE-ID.md
  private static final Pattern SPIFFE_USER_IDENTITY_PATH_PATTERN =
      Pattern.compile("^/v\\d+/user(/.*)?$");

  // Matches LISPIFFE v2 paths and captures the ILM UID (the path after "/v2/").
  // The canonical ILM v2 principal is the full path-after-v2 (e.g. "application/foo-mp/bar-app");
  // ACL matching downstream is segment-prefix on this UID.
  private static final Pattern SPIFFE_V2_PATH_PATTERN = Pattern.compile("^/v2/(.+)$");

  // Matches LISPIFFE v1 workload paths (only "/v1/wl/..."; not "/v1/wf/..." workflow) and
  // captures the app-name. Per LISPIFFE-ID spec, the v1 workload unique-identity is
  // "wl/<app-name>"; we strip the "wl/" type prefix and return just the app-name as the
  // principal, matching how legacy authZ systems handled v1 identities. The app-name is a
  // single path segment (no "/"); a multi-segment value after "wl/" does not match here and
  // falls through to URN/DN extraction instead of being misinterpreted as a single app-name.
  private static final Pattern SPIFFE_V1_WL_PATH_PATTERN = Pattern.compile("^/v1/wl/([^/]+)$");

  @Override
  protected String getConfigPrefix() {
    return X509AuthenticationConfig.SSL_X509_CONFIG_PREFIX;
  }

  @Override
  protected boolean shouldVerifyClientHostname() {
    return false;
  }

  /**
   * Create key manager from config for x509-based authentication provider
   * @param config ZooKeeper config
   * @return An X509KeyManager instance
   */
  public static X509KeyManager createKeyManager(ZKConfig config) {
    try (X509Util x509Util = new ClientX509Util()) {
      String keyStoreLocation = config.getProperty(x509Util.getSslKeystoreLocationProperty(), "");
      String keyStorePassword = config.getProperty(x509Util.getSslKeystorePasswdProperty(), "");
      String keyStoreTypeProp = config.getProperty(x509Util.getSslKeystoreTypeProperty());

      X509KeyManager km = null;
      if (keyStoreLocation.isEmpty()) {
        LOG.warn("Key store location not specified for SSL");
      } else {
        try {
          km = X509Util.createKeyManager(keyStoreLocation, keyStorePassword, keyStoreTypeProp);
        } catch (X509Exception.KeyManagerException e) {
          LOG.error("Failed to create key manager", e);
        }
      }
      return km;
    }
  }

  /**
   * Create trust manager from config for x509-based authentication provider
   * @param config ZooKeeper config
   * @return An X509TrustManager instance
   */
  public static X509TrustManager createTrustManager(ZKConfig config) {
    try (X509Util x509Util = new ClientX509Util()) {
      boolean crlEnabled =
          Boolean.parseBoolean(config.getProperty(x509Util.getSslCrlEnabledProperty()));
      boolean ocspEnabled =
          Boolean.parseBoolean(config.getProperty(x509Util.getSslOcspEnabledProperty()));
      boolean hostnameVerificationEnabled = Boolean
          .parseBoolean(config.getProperty(x509Util.getSslHostnameVerificationEnabledProperty()));
      String trustStoreLocation =
          config.getProperty(x509Util.getSslTruststoreLocationProperty(), "");
      String trustStorePassword = config.getProperty(x509Util.getSslTruststorePasswdProperty(), "");
      String trustStoreTypeProp = config.getProperty(x509Util.getSslTruststoreTypeProperty());

      X509TrustManager tm = null;
      if (trustStoreLocation.isEmpty()) {
        LOG.warn("Truststore location not specified for SSL");
      } else {
        try {
          tm = X509Util
              .createTrustManager(trustStoreLocation, trustStorePassword, trustStoreTypeProp,
                  crlEnabled, ocspEnabled, hostnameVerificationEnabled, false);
        } catch (X509Exception.TrustManagerException e) {
          LOG.error("Failed to create trust manager", e);
        }
      }
      return tm;
    }
  }

  /**
   * Determine the string to be used as the remote host session Id for
   * authorization purposes. Associate this client identifier with a
   * ServerCnxn that has been authenticated over SSL, and any ACLs that refer
   * to the authenticated client.
   *
   * @param clientCert Authenticated X509Certificate associated with the
   *                   remote host.
   * @return Identifier string to be associated with the client.
   *         The clientId can be any string matched and extracted using regex from Subject Distinguished Name or
   *         Subject Alternative Name from x509 certificate.
   *         The clientId string is intended to be an URI for client and map the client to certain domain.
   */
  public static String getClientId(X509Certificate clientCert) {
    // SPIFFE identity extraction always runs, regardless of clientCertIdType configuration —
    // it is not a feature flag. Any URI SAN beginning with "spiffe://" is treated as a
    // candidate; trust in the issuing CA/trust-domain is established upstream by the TLS
    // handshake's trust manager, not by this method.
    try {
      Optional<String> spiffeId = X509AuthenticationUtil.matchAndExtractSpiffeSAN(clientCert);
      if (spiffeId.isPresent()) {
        LOG.debug("Extracted SPIFFE identity: {}", spiffeId.get());
        return spiffeId.get();
      }
    } catch (Exception e) {
      LOG.warn("Failed to extract SPIFFE identity from SAN. Falling through to legacy extraction.", e);
    }

    String clientCertIdType = X509AuthenticationConfig.getInstance().getClientCertIdType();
    if (clientCertIdType != null && clientCertIdType
        .equalsIgnoreCase(X509AuthenticationConfig.SUBJECT_ALTERNATIVE_NAME_SHORT)) {
      try {
        return X509AuthenticationUtil.matchAndExtractSAN(clientCert);
      } catch (Exception ce) {
        LOG.warn("Failed to match and extract a client ID from SAN. Using Subject DN instead.", ce);
      }
    }
    return clientCert.getSubjectX500Principal().getName();
  }

  /**
   * Attempt to extract a client identity from a LISPIFFE URI SAN. Always active — not gated
   * behind any operator configuration. Any URI SAN beginning with {@code spiffe://} is treated
   * as a candidate. Supported forms:
   * <ul>
   *   <li><b>v2</b> ({@code spiffe://<td>/v2/<path>}): principal is the full path-after-{@code /v2/}
   *       (the ILM UID), e.g. {@code spiffe://prod.lipki/v2/application/foo-mp/bar-app} →
   *       {@code application/foo-mp/bar-app}. ACL matching downstream is segment-prefix on the UID.</li>
   *   <li><b>v1 workload</b> ({@code spiffe://<td>/v1/wl/<app-name>}): principal is just the
   *       {@code <app-name>} (the "wl/" type prefix is stripped, matching how legacy authZ
   *       handled v1 identities).</li>
   * </ul>
   *
   * <p>Returns {@link Optional#empty()} when no URI SAN begins with {@code spiffe://}, the
   * matched URI is a user identity ({@code /v<N>/user/...}, which must never be promoted to a
   * service principal), or the matched URI is a non-{v1/wl, v2} path (e.g. v1 workflow
   * {@code /v1/wf/...}). Caller falls through to URN/DN extraction.
   *
   * @throws IllegalArgumentException if multiple URI SANs begin with {@code spiffe://}
   */
  private static Optional<String> matchAndExtractSpiffeSAN(X509Certificate clientCert)
      throws CertificateParsingException {
    String spiffeUri = findSingleMatchingSan(clientCert, 6, SPIFFE_URI_PATTERN, "SPIFFE");
    if (spiffeUri == null) {
      return Optional.empty();
    }

    String path;
    try {
      // getRawPath() returns the literal (un-percent-decoded) path so the principal we accept is
      // exactly what the CA validated in the SAN. getPath() would decode %2F → /, allowing a
      // single-segment SAN like /v2/foo%2Fbar to be promoted to a multi-segment principal that
      // could collide with an unrelated registered identity. Reject any path containing % to
      // also block encoded "user" bypass (e.g. /v2/%75ser/alice).
      path = URI.create(spiffeUri).getRawPath();
    } catch (IllegalArgumentException e) {
      LOG.debug("Malformed SPIFFE URI '{}'; falling through to URN/DN extraction.", spiffeUri);
      return Optional.empty();
    }
    if (path == null) {
      return Optional.empty();
    }
    if (path.indexOf('%') >= 0) {
      LOG.debug("Rejecting SPIFFE URI with percent-encoded path '{}'; falling through.", spiffeUri);
      return Optional.empty();
    }
    if (SPIFFE_USER_IDENTITY_PATH_PATTERN.matcher(path).matches()) {
      LOG.debug("Rejecting SPIFFE user identity '{}' for service-principal extraction.", spiffeUri);
      return Optional.empty();
    }
    Matcher v2Matcher = SPIFFE_V2_PATH_PATTERN.matcher(path);
    if (v2Matcher.matches()) {
      return Optional.of(v2Matcher.group(1));
    }
    Matcher v1WlMatcher = SPIFFE_V1_WL_PATH_PATTERN.matcher(path);
    if (v1WlMatcher.matches()) {
      return Optional.of(v1WlMatcher.group(1));
    }
    LOG.debug("SPIFFE URI '{}' is not a v1/wl or v2 identity; falling through to URN/DN extraction.",
        spiffeUri);
    return Optional.empty();
  }

  /**
   * Returns the single SAN value of the given type whose value matches the regex, or null if
   * there are zero matches. Throws if there are multiple matches (callers always want exactly one).
   */
  private static String findSingleMatchingSan(X509Certificate cert, int sanType, Pattern pattern,
      String matchKind) throws CertificateParsingException {
    String found = null;
    Collection<List<?>> sans = cert.getSubjectAlternativeNames();
    if (sans == null) {
      return null;
    }
    for (List<?> san : sans) {
      if (!Integer.valueOf(sanType).equals(san.get(0))) {
        continue;
      }
      String value = san.get(1).toString();
      if (!pattern.matcher(value).find()) {
        continue;
      }
      if (found != null) {
        String errStr = "Expected exactly 1 " + matchKind + " SAN but found more than 1. "
            + "Please fix the match regex so exactly one match is found.";
        LOG.error(errStr);
        throw new IllegalArgumentException(errStr);
      }
      found = value;
    }
    return found;
  }

  /**
   * Applies an extract regex to a SAN value and returns the captured group.
   *
   * @throws IllegalArgumentException if the regex does not match.
   */
  private static String applyExtractRegex(Pattern extractPattern, String value, int groupIndex) {
    Matcher matcher = extractPattern.matcher(value);
    if (!matcher.find()) {
      String errStr = "Failed to extract identity from '" + value
          + "' using regex '" + extractPattern.pattern() + "'";
      LOG.error(errStr);
      throw new IllegalArgumentException(errStr);
    }
    return matcher.group(groupIndex);
  }

  /**
   * Extract the authenticated client Id from the specified server connection object.
   * @param cnxn Server connection object that contains the certificate.
   * @param trustManager X509 TrustManager for authentication.
   * @return Identifier string to be associated with the client.
   *         The clientId can be any string matched and extracted using regex from Subject Distinguished Name or
   *         Subject Alternative Name from x509 certificate.
   *         The clientId string is intended to be an URI for client and map the client to certain domain.
   * @throws KeeperException.AuthFailedException Failed to authenticate the client certificate
   */
  public static String getClientId(ServerCnxn cnxn, X509TrustManager trustManager)
      throws KeeperException.AuthFailedException {
    X509Certificate clientCert = X509AuthenticationUtil.getAuthenticatedClientCert(cnxn, trustManager);
    return X509AuthenticationUtil.getClientId(clientCert);
  }

  /**
   * Extract SAN field from an X509 certificate
   * @param clientCert Client x509 certificate
   * @return Subject alternative name (SAN) string
   * @throws CertificateParsingException
   */
  private static String matchAndExtractSAN(X509Certificate clientCert)
      throws CertificateParsingException {
    int matchType = X509AuthenticationConfig.getInstance().getClientCertIdSanMatchType();
    String matchRegex = X509AuthenticationConfig.getInstance().getClientCertIdSanMatchRegex();
    String extractRegex = X509AuthenticationConfig.getInstance().getClientCertIdSanExtractRegex();
    int extractMatcherGroupIndex =
        X509AuthenticationConfig.getInstance().getClientCertIdSanExtractMatcherGroupIndex();
    LOG.debug("Using SAN from client cert to extract client ID. matchType: {}, matchRegex: {}, extractRegex: {}, "
            + "extractMatcherGroupIndex: {}", matchType, matchRegex, extractRegex,
        extractMatcherGroupIndex);
    if (matchRegex == null || extractRegex == null || matchType < 0 || matchType > 8) {
      // SAN extension must be in the range of [0, 8].
      // See GeneralName object defined in RFC 5280 (The ASN.1 definition of the SubjectAltName extension)
      String errStr = "Client cert ID type 'SAN' was provided but matchType or matchRegex given is invalid! "
          + "matchType: " + matchType + " matchRegex: " + matchRegex;
      LOG.error(errStr);
      throw new IllegalArgumentException(errStr);
    }
    // filter by match type and match regex
    LOG.debug("Number of SAN entries found in client cert: " + clientCert.getSubjectAlternativeNames().size());
    Pattern matchPattern = Pattern.compile(matchRegex);
    Collection<List<?>> matched = clientCert.getSubjectAlternativeNames().stream().filter(
        list -> list.get(0).equals(matchType) && matchPattern.matcher((CharSequence) list.get(1))
            .find()).collect(Collectors.toList());

    LOG.debug("Number of SAN entries matched: " + matched.size() + ". Printing all matches...");
    for (List<?> match : matched) {
      LOG.debug("Match: (" + match.get(0) + ", " + match.get(1) + ")");
    }

    // if there are more than one match or 0 matches, throw an error
    if (matched.size() != 1) {
      String errStr = "Zero or multiple matches found in SAN! Please fix match type and regex so that exactly one match "
          + "is found.";
      LOG.error(errStr);
      throw new IllegalArgumentException(errStr);
    }

    return applyExtractRegex(Pattern.compile(extractRegex),
        matched.iterator().next().get(1).toString(), extractMatcherGroupIndex);
  }

  /**
   * Get a client certificate from server connection object and authenticate the certificate using X509TrustManager
   * @param cnxn ServerCnxn object
   * @param trustManager
   * @return The authenticated client certificate
   * @throws KeeperException.AuthFailedException Failed to authenticate the client certificate
   */
  public static X509Certificate getAuthenticatedClientCert(ServerCnxn cnxn, X509TrustManager trustManager)
      throws KeeperException.AuthFailedException {
    X509Certificate[] certChain = (X509Certificate[]) cnxn.getClientCertificateChain();

    if (certChain == null || certChain.length == 0) {
      String errMsg = "No X509 certificate is found in cert chain.";
      LOG.error(errMsg);
      throw new KeeperException.AuthFailedException();
    }

    X509Certificate clientCert = certChain[0];

    if (trustManager == null) {
      String errMsg = "No trust manager available to authenticate session 0x" + Long.toHexString(cnxn.getSessionId());
      LOG.error(errMsg);
      throw new KeeperException.AuthFailedException();
    }

    try {
      // Authenticate client certificate
      trustManager.checkClientTrusted((X509Certificate[]) cnxn.getClientCertificateChain(), clientCert.getPublicKey().getAlgorithm());
    } catch (CertificateException ce) {
      String errMsg = "Failed to trust certificate for session 0x" + Long.toHexString(cnxn.getSessionId());
      LOG.error(errMsg, ce);
      throw new KeeperException.AuthFailedException();
    }
    return certChain[0];
  }
}
