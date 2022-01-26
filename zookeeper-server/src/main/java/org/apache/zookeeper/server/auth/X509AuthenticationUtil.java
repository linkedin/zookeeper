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

import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;
import org.apache.zookeeper.common.ClientX509Util;
import org.apache.zookeeper.common.X509Exception;
import org.apache.zookeeper.common.X509Util;
import org.apache.zookeeper.common.ZKConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Util class for x509 certificate-based authentication providers.
 */
public class X509AuthenticationUtil extends X509Util {

  private static final Logger LOG = LoggerFactory.getLogger(X509AuthenticationUtil.class);
  /**
   * The following System Property keys are used to extract clientId from the client cert.
   */
  public static final String SSL_X509_CLIENT_CERT_ID_TYPE = "zookeeper.ssl.x509.clientCertIdType";
  public static final String SSL_X509_CLIENT_CERT_ID_SAN_MATCH_TYPE =
      "zookeeper.ssl.x509.clientCertIdSanMatchType";
  // Match Regex is used to choose which entry to use, default value ".*"
  public static final String SSL_X509_CLIENT_CERT_ID_SAN_MATCH_REGEX =
      "zookeeper.ssl.x509.clientCertIdSanMatchRegex";
  // Extract Regex is used to construct a client ID (acl entity) to return, default value ".*"
  public static final String SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_REGEX =
      "zookeeper.ssl.x509.clientCertIdSanExtractRegex";
  // Specifies match group index for the extract regex (i in Matcher.group(i)), default value 0
  public static final String SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_MATCHER_GROUP_INDEX =
      "zookeeper.ssl.x509.clientCertIdSanExtractMatcherGroupIndex";
  public static final String SUBJECT_ALTERNATIVE_NAME_SHORT = "SAN";

  @Override
  protected String getConfigPrefix() {
    return "zookeeper.ssl.x509.";
  }

  @Override
  protected boolean shouldVerifyClientHostname() {
    return false;
  }

  public static X509KeyManager createKeyManager(ZKConfig config) {
    try (X509Util x509Util = new ClientX509Util()) {
      String keyStoreLocation = config.getProperty(x509Util.getSslKeystoreLocationProperty(), "");
      String keyStorePassword = config.getProperty(x509Util.getSslKeystorePasswdProperty(), "");
      String keyStoreTypeProp = config.getProperty(x509Util.getSslKeystoreTypeProperty());

      X509KeyManager km = null;
      if (keyStoreLocation.isEmpty()) {
        LOG.warn("X509AuthenticationUtil::keystore not specified for client connection");
      } else {
        try {
          km = X509Util.createKeyManager(keyStoreLocation, keyStorePassword, keyStoreTypeProp);
        } catch (X509Exception.KeyManagerException e) {
          LOG.error("X509AuthenticationUtil::Failed to create key manager", e);
        }
      }
      return km;
    }
  }

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
        LOG.warn("X509AuthenticationUtil::Truststore not specified for client connection");
      } else {
        try {
          tm = X509Util
              .createTrustManager(trustStoreLocation, trustStorePassword, trustStoreTypeProp,
                  crlEnabled, ocspEnabled, hostnameVerificationEnabled, false);
        } catch (X509Exception.TrustManagerException e) {
          LOG.error("X509AuthenticationUtil::Failed to create trust manager", e);
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
   */
  public static String getClientId(X509Certificate clientCert) {
    String clientCertIdType =
        System.getProperty(X509AuthenticationUtil.SSL_X509_CLIENT_CERT_ID_TYPE);
    if (clientCertIdType != null && clientCertIdType.equalsIgnoreCase(SUBJECT_ALTERNATIVE_NAME_SHORT)) {
      try {
        return X509AuthenticationUtil.matchAndExtractSAN(clientCert);
      } catch (Exception ce) {
        LOG.warn("X509AuthenticationUtil::getClientId(): failed to match and extract a"
            + " client ID from SAN! Using Subject DN instead...", ce);
      }
    }
    // return Subject DN by default
    return clientCert.getSubjectX500Principal().getName();
  }

  private static String matchAndExtractSAN(X509Certificate clientCert)
      throws CertificateParsingException {
    Integer matchType = Integer.getInteger(SSL_X509_CLIENT_CERT_ID_SAN_MATCH_TYPE);
    String matchRegex = System.getProperty(SSL_X509_CLIENT_CERT_ID_SAN_MATCH_REGEX, ".*");
    String extractRegex = System.getProperty(SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_REGEX, ".*");
    Integer extractMatcherGroupIndex =
        Integer.getInteger(SSL_X509_CLIENT_CERT_ID_SAN_EXTRACT_MATCHER_GROUP_INDEX);
    LOG.info("X509AuthenticationUtil::matchAndExtractSAN(): Using SAN in the client cert "
            + "for client ID! matchType: {}, matchRegex: {}, extractRegex: {}, "
            + "extractMatcherGroupIndex: {}", matchType, matchRegex, extractRegex,
        extractMatcherGroupIndex);
    if (matchType == null || matchRegex == null || extractRegex == null || matchType < 0
        || matchType > 8) {
      // SAN extension must be in the range of [0, 8].
      // See GeneralName object defined in RFC 5280 (The ASN.1 definition of the
      // SubjectAltName extension)
      String errStr = "X509AuthenticationUtil::matchAndExtractSAN(): ClientCert ID type "
          + "SAN was provided but matchType or matchRegex given is invalid! matchType: " + matchType
          + " matchRegex: " + matchRegex;
      LOG.error(errStr);
      throw new IllegalArgumentException(errStr);
    }
    // filter by match type and match regex
    LOG.info("X509AuthenticationUtil::matchAndExtractSAN(): number of SAN entries found in" + " clientCert: " + clientCert
        .getSubjectAlternativeNames().size());
    Pattern matchPattern = Pattern.compile(matchRegex);
    Collection<List<?>> matched = clientCert.getSubjectAlternativeNames().stream().filter(
        list -> list.get(0).equals(matchType) && matchPattern.matcher((CharSequence) list.get(1))
            .find()).collect(Collectors.toList());

    LOG.info("X509AuthenticationUtil::matchAndExtractSAN(): number of SAN entries matched: " + matched.size()
        + ". Printing all matches...");
    for (List<?> match : matched) {
      LOG.info("  Match: (" + match.get(0) + ", " + match.get(1) + ")");
    }

    // if there are more than one match or 0 matches, throw an error
    if (matched.size() != 1) {
      String errStr = "X509AuthenticationUtil::matchAndExtractSAN(): 0 or multiple "
          + "matches found in SAN! Please fix match type and regex so that exactly one match "
          + "is found.";
      LOG.error(errStr);
      throw new IllegalArgumentException(errStr);
    }

    // Extract a substring from the found match using extractRegex
    Pattern extractPattern = Pattern.compile(extractRegex);
    Matcher matcher = extractPattern.matcher(matched.iterator().next().get(1).toString());
    if (matcher.find()) {
      // If extractMatcherGroupIndex is not given, return the 1st index by default
      extractMatcherGroupIndex = extractMatcherGroupIndex == null ? 0 : extractMatcherGroupIndex;
      String result = matcher.group(extractMatcherGroupIndex);
      LOG.info("X509AuthenticationUtil::matchAndExtractSAN(): returning extracted "
          + "client ID: {} using Matcher group index: {}", result, extractMatcherGroupIndex);
      return result;
    }
    String errStr = "X509AuthenticationUtil::matchAndExtractSAN(): failed to find an "
        + "extract substring! Please review the extract regex!";
    LOG.error(errStr);
    throw new IllegalArgumentException(errStr);
  }
}
