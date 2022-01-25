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

package org.apache.zookeeper.server.auth.znode.groupacl;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Set;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.common.ClientX509Util;
import org.apache.zookeeper.common.X509Exception;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.auth.ServerAuthenticationProvider;
import org.apache.zookeeper.server.auth.X509AuthenticationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A ServerAuthenticationProvider implementation that does both authentication and authorization for protecting znodes from unauthorized access.
 * Znodes are grouped into domains according to their ownership, and clients are granted access permission to domains.
 * Authentication mechanism is same as in X509AuthenticationProvider.
 * Authorization is done by checking with clients' URI (uniform resource identifier) in ACL metadata for matched domains.
 */
public class X509ZNodeGroupAclProvider extends ServerAuthenticationProvider {

  private static final Logger LOG = LoggerFactory.getLogger(X509ZNodeGroupAclProvider.class);
  private final String logStrPrefix = this.getClass().getName() + ":: ";
  private final X509TrustManager trustManager;
  private final X509KeyManager keyManager;

  public X509ZNodeGroupAclProvider()
      throws X509Exception.KeyManagerException, X509Exception.TrustManagerException {
    // Reuse logic in X509AuthenticationProvider
    X509AuthenticationProvider authProvider = new X509AuthenticationProvider();
    this.keyManager = authProvider.getKeyManager();
    this.trustManager = authProvider.getTrustManager();
  }

  public X509ZNodeGroupAclProvider(X509TrustManager trustManager, X509KeyManager keyManager) {
    this.trustManager = trustManager;
    this.keyManager = keyManager;
  }

  @Override
  public KeeperException.Code handleAuthentication(ServerObjs serverObjs, byte[] authData) {
    // Get x509 certificate
    ServerCnxn cnxn = serverObjs.getCnxn();
    X509Certificate[] certChain = (X509Certificate[]) cnxn.getClientCertificateChain();
    if (certChain == null || certChain.length == 0) {
      LOG.error(logStrPrefix + "No x509 certificate is found.");
      return KeeperException.Code.AUTHFAILED;
    }
    X509Certificate clientCert = certChain[0];

    if (trustManager == null) {
      LOG.error(logStrPrefix + "No trust manager available to authenticate session 0x{}",
          Long.toHexString(cnxn.getSessionId()));
      return KeeperException.Code.AUTHFAILED;
    }

    try {
      // Authenticate client certificate
      trustManager.checkClientTrusted(certChain, clientCert.getPublicKey().getAlgorithm());
    } catch (CertificateException ce) {
      LOG.error(logStrPrefix + "Failed to trust certificate for session 0x{}",
          Long.toHexString(cnxn.getSessionId()), ce);
      return KeeperException.Code.AUTHFAILED;
    }

    // Extract URI from certificate
    String clientId;
    try (ClientX509Util x509Util = new ClientX509Util()) {
      clientId = x509Util.getClientId(clientCert);
    } catch (Exception e) {
      // Failed to extract URI from certificate
      LOG.error(logStrPrefix + "Failed to extract URI from certificate for session 0x{}",
          Long.toHexString(cnxn.getSessionId()), e);
      return KeeperException.Code.OK;
    }

    // User belongs to super user group
    if (clientId.equals(System
        .getProperty(X509AuthenticationProvider.ZOOKEEPER_X509AUTHENTICATIONPROVIDER_SUPERUSER))) {
      cnxn.addAuthInfo(new Id("super", clientId));
      LOG.info("Authenticated Id '{}' as super user", clientId);

      return KeeperException.Code.OK;
    }

    // Get authorized domain names for client
    ClientUriDomainMappingHelper uriDomainMappingHelper =
        new ZkClientUriDomainMappingHelper(serverObjs.getZks());
    Set<String> domains = uriDomainMappingHelper.getDomains(clientId);
    if (domains.isEmpty()) {
      // If no domain name is found, use URI as domain name
      domains.add(clientId);
    }

    Set<String> superUserDomainNames = ZNodeGroupAclUtil.getSuperUserDomainNames();
    for (String domain : domains) {
      // Grant cross domain components super user privilege
      if (superUserDomainNames.contains(domain)) {
        cnxn.addAuthInfo(new Id("super", clientId));
        LOG.info(logStrPrefix + "Authenticated Id '{}' as super user", clientId);
      } else {
        cnxn.addAuthInfo(new Id(getScheme(), domain));
        LOG.info(logStrPrefix + "Authenticated Id '{}' for Scheme '{}', Domain '{}'.", clientId,
            getScheme(), domain);
      }
    }

    return KeeperException.Code.OK;
  }

  @Override
  public boolean matches(ServerObjs serverObjs, MatchValues matchValues) {
    for (Id id : serverObjs.getCnxn().getAuthInfo()) {
      if (id.getId().equals(matchValues.getAclExpr()) || id.getId().equals(System.getProperty(
          X509AuthenticationProvider.ZOOKEEPER_X509AUTHENTICATIONPROVIDER_SUPERUSER))) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String getScheme() {
    return "x509";
  }

  @Override
  public boolean isAuthenticated() {
    return true;
  }

  @Override
  public boolean isValid(String id) {
    try {
      new X500Principal(id);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }
}
