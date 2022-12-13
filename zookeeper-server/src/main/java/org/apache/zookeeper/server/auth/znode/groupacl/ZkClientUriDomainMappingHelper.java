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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.server.DumbWatcher;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.auth.X509AuthenticationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of ClientUriDomainMappingHelper that stores the mapping inside the ZK server
 * as a hierarchy of ZNodes.
 *
 * Note that the mapping metadata itself will be stored in ZKDatabase as a ZNode tree and will also
 * be cached inside this helper object. This helper object watches the clientUri-domain ZNodes and
 * updates the internal Map accordingly.
 *
 * The following illustrates the ZNode hierarchy:
 * . (root)
 * └── /zookeeper/uri-domain-map (mapping root path)
 *     ├── bar (application domain)
 *     │   ├── bar0 (client URI)
 *     │   └── bar1 (client URI)
 *     └── foo (application domain)
 *         ├── foo1 (client URI)
 *         ├── foo2 (client URI)
 *         └── foo3 (client URI)
 *
 * Note: It is not expected that there would be too many distinct client URIs so as to overwhelm
 * heap usage.
 */
public class ZkClientUriDomainMappingHelper implements ClientUriDomainMappingHelper {

  private static final Logger LOG = LoggerFactory.getLogger(ZkClientUriDomainMappingHelper.class);

  private final ZooKeeperServer zks;
  private final String rootPath;

  private Map<String, Set<String>> clientUriToDomainNames = Collections.emptyMap();
  private ConnectionAuthInfoUpdater updater = null;

  public ZkClientUriDomainMappingHelper(ZooKeeperServer zks) {
    this.zks = zks;

    this.rootPath =
        X509AuthenticationConfig.getInstance().getZnodeGroupAclClientUriDomainMappingRootPath();
    LOG.info("Client URI domain mapping root path: {}", this.rootPath);
    if (rootPath == null) {
      throw new IllegalStateException(
          "Client URI domain mapping root path config is not set!");
    }

    if (zks.getZKDatabase().getNode(rootPath) == null) {
      LOG.warn("Client URI domain mapping root path {} does not exist.", this.rootPath);
    }

    addWatches();
    parseZNodeMapping();
  }

  /**
   * @return True if the new updater is setup to the helper instance. False if the specified updater is not set since
   * another updater has already been configured.
   */
  boolean setDomainAuthUpdater(ConnectionAuthInfoUpdater updater) {
    if (this.updater == null) {
      synchronized (this) {
        if (this.updater == null) {
          this.updater = updater;
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Install a persistent recursive watch on the root path.
   */
  private void addWatches() {
    zks.getZKDatabase().addWatch(rootPath, new MappingRootWatcher(), ZooDefs.AddWatchModes.persistentRecursive);
  }

  /**
   * Read ZNodes under the root path and populates clientUriToDomainNames.
   * Note: this is not thread-safe nor atomic; however, we do not need such strong guarantee with
   * this read operation.
   *
   * Also, note that this is a purely in-memory operation, so re-parsing the entire tree should not
   * be a big overhead considering how infrequently the mapping is supposed to be changed.
   */
  private void parseZNodeMapping() {
    Map<String, Set<String>> newClientUriToDomainNames = new HashMap<>();
    try {
      List<String> domainNames = zks.getZKDatabase().getChildren(rootPath, null, null);
      domainNames.forEach(domainName -> {
        try {
          List<String> clientUris =
              zks.getZKDatabase().getChildren(rootPath + "/" + domainName, null, null);
          clientUris.forEach(clientUri -> {
              LOG.info("Registering client URI domain mapping: {} --> {}", clientUri, domainName);
              newClientUriToDomainNames.computeIfAbsent(clientUri, k -> new HashSet<>()).add(domainName);
          });
        } catch (KeeperException.NoNodeException e) {
          LOG.warn("No clientUri ZNodes found under domain: {}", domainName);
        }
      });
    } catch (KeeperException.NoNodeException e) {
      LOG.warn("No application domain ZNodes found in root path: {}", rootPath);
    }
    clientUriToDomainNames = newClientUriToDomainNames;
  }

  @Override
  public Set<String> getDomains(String clientUri) {
    return clientUriToDomainNames.getOrDefault(clientUri, Collections.emptySet());
  }

  @Override
  public void updateDomainBasedAuthInfo(ServerCnxn cnxn) {
    if (updater != null && cnxn != null) {
      // UpdateAuthInfo is triggered on new connection, as well as any URI-domain map ZNode changes.
      // To prevent inconsistent update, concurrency control is necessary.
      synchronized (updater) {
        updater.updateAuthInfo(cnxn, clientUriToDomainNames);
      }
    }
  }

  /**
   * The watcher used to listen on client uri - domain mapping root path for mapping update
   * Extends DumbWatcher instead of ServerCnxn here some methods in ServerCnxn are not accessible from here
   */
  public class MappingRootWatcher extends DumbWatcher {
    @Override
    public void process(WatchedEvent event) {
      LOG.info("Processing watched event: {}", event.toString());
      parseZNodeMapping();
      // Update AuthInfo for all the known connections.
      // Note : It is not ideal to iterate over all plaintext connections which are connected over non-TLS but right now
      // there is no way to find out if connection on unified port is using SSLHandler or nonSSLHandler. Anyways, we
      // should not ideally have any nonSSLHandler connections on unified port after complete rollout.

      // TODO Change to read SecureServerCnxnFactory only. The current logic is to support unit test who is not creating
      // a secured server cnxn factory. It won't cause any problem but is not technically correct.

      // Since port unification is supported, TLS requests could be made on unified as well as secure port. Hence iterate
      // over all connections to update auth info.
      ServerCnxnFactory factory = zks.getServerCnxnFactory();
      LOG.info("Updating auth info for connections");
      // TODO Evaluate performance impact and potentially use thread pool to parallelize the AuthInfo update.
      if (factory != null) {
        factory.getConnections().forEach(cnxn -> updateDomainBasedAuthInfo(cnxn));
      }
      ServerCnxnFactory secureFactory = zks.getSecureServerCnxnFactory();
      LOG.info("Updating auth info for TLS connections");
      if (secureFactory != null) {
        secureFactory.getConnections().forEach(cnxn -> updateDomainBasedAuthInfo(cnxn));
      }
    }
  }
}
