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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.common.ZKConfig;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.auth.ServerAuthenticationProvider;
import org.apache.zookeeper.server.auth.X509AuthenticationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A ServerAuthenticationProvider implementation that does both authentication and authorization for protecting znodes from unauthorized access.
 * Znodes are grouped into domains according to their ownership, and clients are granted access permission to domains.
 * Authentication mechanism is same as in X509AuthenticationProvider. If authentication is failed,
 *  decline the connection request.
 * Authorization is done by checking clientId which is usually an URI (uniform resource identifier)in ACL metadata for matched domains.
 * Detailed step for authorization is:
 *  1. Acl provider attempts to extract the clientId from the cert provided by the client
 *  2. Acl provider attempts to look up the client's domain using the clientId and ClientUriDomainMappingHelper
 *  3. If matched domain is found, add the domain in authInfo in the connection object;
 *     if no matched domain is found, add the clientId in authInfo instead; either way, establish the connection.
 * This class is meant to support the following use patterns:
 *  1. Single domain: The creator and the accessor to the znodes belong to the same domain. This is
 *     the most straightforward use case, and accessors in other domains won't be able to access the
 *     znodes due to their clientId not matching any in the znodes.
 *  2. Super user domain: The accessors need permission to access znodes created by creators belong
 *     to multiple domains. In this case the accessors will be mapped to super user domain and be
 *     given super user privilege.
 *  3. Open read access: The znodes need to be accessed by accessors from many different domains, so when
 *     the creators create these znodes, these nodes will be given "open read access" (see below).
 * Optional features include:
 *    "auto-set ACL": add ZooDefs.Ids.CREATOR_ALL_ACL to all newly-created znodes,
 *    "open read access": this feature concerns read access only. Add (world, anyone r) to all
 *          newly-written znodes whose path prefixes are given in the znode group acl config
 *          (comma-delimited, multiple such prefixes are possible).
 */
public class X509ZNodeGroupAclProvider extends ServerAuthenticationProvider {
  private static final Logger LOG = LoggerFactory.getLogger(X509ZNodeGroupAclProvider.class);
  private final String logStrPrefix = this.getClass().getName() + ":: ";
  private final X509TrustManager trustManager;
  private final X509KeyManager keyManager;

  // Although using "volatile" keyword with double checked locking could prevent the undesired
  //creation of multiple objects; not using here for the consideration of read performance
  private ClientUriDomainMappingHelper uriDomainMappingHelper = null;
  private static final String ZOOKEEPER_ZNODEGROUPACL_SUPERUSER_AUTH_SCHEME = "super";
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
    // Authenticate connection
    ServerCnxn cnxn = serverObjs.getCnxn();
    try {
      X509AuthenticationUtil.getAuthenticatedClientCert(cnxn, trustManager);
    } catch (KeeperException.AuthFailedException e) {
      return KeeperException.Code.AUTHFAILED;
    } catch (Exception e) {
      // Failed to extract clientId from certificate
      LOG.error(logStrPrefix + "Failed to extract URI from certificate for session 0x{}",
          Long.toHexString(cnxn.getSessionId()), e);
      return KeeperException.Code.OK;
    }

    try {
      ClientUriDomainMappingHelper helper = getUriDomainMappingHelper(serverObjs.getZks());
      // Initially assign AuthInfo to the new connection by triggering the helper update method.
      // Note the assumption is that the connection has been registered in the ServerCnxnFactory before ZookeeperServer
      // triggers handleAuthentication call. This is true because first auth request is sent after connection establish
      // is done. If this design is changed in the future, then triggering overall AuthInfo update for all the "known"
      // connections won't work.
      helper.updateAuthInfoDomains(cnxn);
    } catch (Exception e) {
      LOG.error(logStrPrefix + "Failed to authorize session 0x{}", Long.toHexString(cnxn.getSessionId()), e);
    }

    // Authentication is done regardless of whether authorization is done or not.
    return KeeperException.Code.OK;
  }

  @Override
  public boolean matches(ServerObjs serverObjs, MatchValues matchValues) {
    for (Id id : serverObjs.getCnxn().getAuthInfo()) {
      // Not checking for super user here because the check is already covered
      // in checkAcl() in ZookeeperServer.class
      if (id.getId().equals(matchValues.getAclExpr())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String getScheme() {
    // Same scheme as X509AuthenticationProvider since they both use x509 certificate
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

  /**
   * Initialize a new ClientUriDomainMappingHelper instance if no one has been instantiated for the ACL provider.
   * @param zks
   */
  private ClientUriDomainMappingHelper getUriDomainMappingHelper(ZooKeeperServer zks) {
    if (uriDomainMappingHelper == null) {
      synchronized (this) {
        if (uriDomainMappingHelper == null) {
          ZkClientUriDomainMappingHelper helper = new ZkClientUriDomainMappingHelper(zks);
          // Set up AuthInfo updater to refresh connection AuthInfo on any client domain changes.
          helper.setDomainAuthUpdater((cnxn, clientUriToDomainNames) -> {
            try {
              String clientId = X509AuthenticationUtil.getClientId(cnxn, trustManager);
              assignAuthInfo(cnxn, clientId,
                  // If no domain name is found, use cleint Id as default domain name
                  clientUriToDomainNames.getOrDefault(clientId, Collections.singleton(clientId)));
            } catch (UnsupportedOperationException unsupportedEx) {
              LOG.info(logStrPrefix + "Cannot update AuthInfo for session 0x{} since operations are not supported.",
                  Long.toHexString(cnxn.getSessionId()));
            } catch (Exception e) {
              LOG.error(logStrPrefix + "Failed to update AuthInfo for session 0x{}. Revoking all of it's AuthInfo.",
                  Long.toHexString(cnxn.getSessionId()), e);
              try {
                cnxn.getAuthInfo().stream().forEach(id -> cnxn.removeAuthInfo(id));
              } catch (Exception ex) {
                LOG.error(logStrPrefix + "Failed to revoke AuthInfo for session 0x{}.",
                    Long.toHexString(cnxn.getSessionId()), ex);
              }
            }
          });
          uriDomainMappingHelper = helper;
          LOG.info(logStrPrefix + "New UriDomainMappingHelper has been instantiated.");
        }
      }
    }
    return uriDomainMappingHelper;
  }

  /**
   * Assign AuthInfo to the specified connection.
   * Note, do not use this method outside ConnectionAuthInfoUpdater.updateAuthInfo implementation. Otherwise,
   * concurrency control is required to prevent inconsistent update.
   *
   * @param cnxn Client connection to be updated
   * @param clientId ClientId to be potentially used as the AuthInfo Id if the client is super user.
   *                 The clientId can be any string matched and extracted using regex from Subject Distinguished
   *                 Name or Subject Alternative Name from x509 certificate.
   *                 The clientId string is intended to be an URI for client and map the client to certain domain.
   *                 The user can use the properties defined in X509AuthenticationUtil to extract a desired string as
   *                 clientId.
   * @param domains Domains to be used as the AuthInfo Id.
   */
  private void assignAuthInfo(ServerCnxn cnxn, String clientId, Set<String> domains) {
    Set<String> superUserDomainNames = ZNodeGroupAclProperties.getInstance().getSuperUserDomainNames();
    String superUser = System.getProperty(ZOOKEEPER_ZNODEGROUPACL_SUPERUSER);

    Set<Id> newAuthIds = new HashSet<>();
    // Check if user belongs to super user group
    if (clientId.equals(superUser) || superUserDomainNames.stream().anyMatch(d -> domains.contains(d))) {
      newAuthIds.add(new Id(ZOOKEEPER_ZNODEGROUPACL_SUPERUSER_AUTH_SCHEME, clientId));
    }
    // Assign Auth Id according to domains
    domains.stream().forEach(d -> newAuthIds.add(new Id(getScheme(), d)));

    // Update the existing connection AuthInfo accordingly.
    Set<Id> currentCnxnAuthIds = new HashSet<>(cnxn.getAuthInfo());
    currentCnxnAuthIds.stream().forEach(id -> {
      if (id.getScheme().equals(ZOOKEEPER_ZNODEGROUPACL_SUPERUSER_AUTH_SCHEME) || id.getScheme().equals(getScheme())) {
        if (!newAuthIds.contains(id)) {
          cnxn.removeAuthInfo(id);
          LOG.info(logStrPrefix + "Authenticated Id '{}' has been removed from session 0x{}.", id,
              Long.toHexString(cnxn.getSessionId()));
        }
      } // else, the Auth Id was not assigned by this provider, don't touch.
    });

    newAuthIds.stream().forEach(id -> {
      if (!currentCnxnAuthIds.contains(id)) {
        cnxn.addAuthInfo(id);
        LOG.info(logStrPrefix + "Authenticated Id '{}' has been added to session 0x{}.", id, cnxn.getSessionId());
      }
    });
  }
}
