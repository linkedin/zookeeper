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

import java.security.cert.X509Certificate;
import java.util.HashSet;
import java.util.Set;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.common.ZKConfig;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.auth.ServerAuthenticationProvider;
import org.apache.zookeeper.server.auth.X509AuthenticationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A ServerAuthenticationProvider implementation that does both authentication and authorization for protecting znodes from unauthorized access.
 * Znodes are grouped into domains according to their ownership, and clients are granted access permission to domains.
 * Authentication mechanism is same as in X509AuthenticationProvider.
 * Authorization is done by checking with clients' URI (uniform resource identifier) in ACL metadata for matched domains.
 * This class is meant to support the following use patterns:
 *  1.Multiple creators, one reader - ZNodes, created by multiple identities from multiple domains,
 *    need to be read by an identity from a different domain.
 *  2.Multiple creators, multiple readers - ZNodes, created by multiple identities from multiple domains,
 *    need to be read by multiple identities from multiple domains.
 *  3.Multiple creators, one all-permission accessor - ZNodes, created by multiple identities from multiple domains,
 *    need to be read/update/deleted by an identity from a different domain.
 * Optional features include:
 *    set client Id as znode ACL when creating nodes,
 *    add additional public read ACL to znodes based on path.
 */
public class X509ZNodeGroupAclProvider extends ServerAuthenticationProvider {

  private static final Logger LOG = LoggerFactory.getLogger(X509ZNodeGroupAclProvider.class);
  private final String logStrPrefix = this.getClass().getName() + ":: ";
  private final X509TrustManager trustManager;
  private final X509KeyManager keyManager;
  static final String ZOOKEEPER_ZNODEGROUPACL_SUPERUSER = "zookeeper.znodeGroupAcl.superUser";

  public X509ZNodeGroupAclProvider() {
    ZKConfig config = new ZKConfig();
    this.keyManager = X509AuthenticationUtil.createKeyManager(config);
    this.trustManager = X509AuthenticationUtil.createTrustManager(config);
  }

  public X509ZNodeGroupAclProvider(X509TrustManager trustManager, X509KeyManager keyManager) {
    this.trustManager = trustManager;
    this.keyManager = keyManager;
  }

  @Override
  public KeeperException.Code handleAuthentication(ServerObjs serverObjs, byte[] authData) {
    ServerCnxn cnxn = serverObjs.getCnxn();
    X509Certificate clientCert;
    try {
      clientCert = X509AuthenticationUtil.getAndAuthenticateClientCert(cnxn, trustManager);
    } catch (KeeperException.AuthFailedException e) {
      return KeeperException.Code.AUTHFAILED;
    }

    // Extract URI from certificate
    String uri;
    try {
      uri = X509AuthenticationUtil.getClientId(clientCert);
    } catch (Exception e) {
      // Failed to extract URI from certificate
      LOG.error(logStrPrefix + "Failed to extract URI from certificate for session 0x{}",
          Long.toHexString(cnxn.getSessionId()), e);
      return KeeperException.Code.OK;
    }

    // User belongs to super user group
    if (uri.equals(System.getProperty(ZOOKEEPER_ZNODEGROUPACL_SUPERUSER))) {
      cnxn.addAuthInfo(new Id("super", uri));
      LOG.info("Authenticated Id '{}' as super user", uri);
      return KeeperException.Code.OK;
    }

    // Get authorized domain names for client
    ClientUriDomainMappingHelper uriDomainMappingHelper =
        new ZkClientUriDomainMappingHelper(serverObjs.getZks());
    Set<String> domains = uriDomainMappingHelper.getDomains(uri);
    if (domains.isEmpty()) {
      // If no domain name is found, use URI as domain name
      domains = new HashSet<>();
      domains.add(uri);
    }

    Set<String> superUserDomainNames = ZNodeGroupAclProperties.getInstance().getSuperUserDomainNames();
    for (String domain : domains) {
      // Grant cross domain components super user privilege
      if (superUserDomainNames.contains(domain)) {
        cnxn.addAuthInfo(new Id("super", uri));
        LOG.info(logStrPrefix + "Id '{}' belongs to superUser domain '{}', authenticated as super user", uri,
            domain);
      } else {
        cnxn.addAuthInfo(new Id(getScheme(), domain));
        LOG.info(logStrPrefix + "Authenticated Id '{}' for Scheme '{}', Domain '{}'.", uri,
            getScheme(), domain);
      }
    }

    return KeeperException.Code.OK;
  }

  @Override
  public boolean matches(ServerObjs serverObjs, MatchValues matchValues) {
    for (Id id : serverObjs.getCnxn().getAuthInfo()) {
      if (id.getId().equals(matchValues.getAclExpr()) || id.getId()
          .equals(System.getProperty(ZOOKEEPER_ZNODEGROUPACL_SUPERUSER))) {
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
