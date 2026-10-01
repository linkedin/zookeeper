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

import com.google.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
 * Each <b>leaf</b> znode below a domain is registered as a client URI; the URI is the
 * {@code /}-joined path of znode names from the domain down. Since znode names themselves
 * cannot contain {@code /}, multi-segment SPIFFE ILM UIDs
 * ({@code application/<mp>/<app>[/<tag>]}) are expressed as a path of nested znodes. Intermediate
 * znodes are structural only — they are not registered as keys, which prevents a stray
 * single-segment znode (e.g. {@code workload}) from matching every SPIFFE workload identity.
 *
 * <p>Example tree:
 * <pre>
 * /zookeeper/uri-domain-map
 * ├── bar
 * │   └── urn:li:servicePrincipal(bar;ei4;i001)            → "urn:li:servicePrincipal(bar;ei4;i001)" → bar
 * └── helix
 *     └── workload
 *         └── helix-core
 *             ├── helix-controller                          → "workload/helix-core/helix-controller" → helix
 *             └── helix-rest                                → "workload/helix-core/helix-rest"       → helix
 * </pre>
 *
 * To grant an MP-level prefix instead, register the MP node as a leaf (i.e. omit app-level
 * children); the segment-prefix walk-up in {@link #getDomains(String)} then matches any UID
 * with that prefix.
 *
 * Note: It is not expected that there would be too many distinct client URIs so as to overwhelm
 * heap usage.
 */
public class ZkClientUriDomainMappingHelper implements ClientUriDomainMappingHelper {

  private static final Logger LOG = LoggerFactory.getLogger(ZkClientUriDomainMappingHelper.class);

  private final ZooKeeperServer zks;
  private final String rootPath;

  // volatile to publish the reassignment in parseZNodeMapping (watcher thread) to readers in
  // getDomains (request-handler threads); see allowedClientIdAsAclDomains in X509AuthenticationConfig
  // for the same pattern.
  private volatile Map<String, Set<String>> clientUriToDomainNames = Collections.emptyMap();
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
  @SuppressFBWarnings("DC_DOUBLECHECK")
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
   * The watcher has to be added here instead of in {@link org.apache.zookeeper.server.auth.znode.groupacl.X509ZNodeGroupAclProvider}
   * because this class is one layer above connections, and is a central place to handle add watch,
   * process event, etc logic for all the connections in this server.
   * Therefore, only one watch needs to be added per server.
   */
  private void addWatches() {
    zks.getZKDatabase().addWatch(rootPath, new MappingRootWatcher(), ZooDefs.AddWatchModes.persistentRecursive);
  }

  /**
   * Re-read the entire mapping subtree and swap in a new {@code clientUriToDomainNames}. See
   * class Javadoc for the registration rule. Runs on bootstrap and on watcher fire (infrequent),
   * purely in-memory. Not thread-safe with itself; the volatile reassignment publishes a
   * consistent map to readers.
   */
  private void parseZNodeMapping() {
    Map<String, Set<String>> newClientUriToDomainNames = new HashMap<>();
    try {
      List<String> domainNames = zks.getZKDatabase().getChildren(rootPath, null, null);
      for (String domainName : domainNames) {
        collectClientUris(rootPath + "/" + domainName, "", domainName, newClientUriToDomainNames);
      }
    } catch (KeeperException.NoNodeException e) {
      LOG.warn("No application domain ZNodes found in root path: {}", rootPath);
    }
    clientUriToDomainNames = newClientUriToDomainNames;
  }

  private void collectClientUris(String currentPath, String accumulatedUri, String domainName,
      Map<String, Set<String>> map) {
    List<String> children;
    try {
      children = zks.getZKDatabase().getChildren(currentPath, null, null);
    } catch (KeeperException.NoNodeException e) {
      return;
    }
    if (children.isEmpty()) {
      // Only leaf znodes are registered as client URIs. Intermediate znodes are structural —
      // registering them would grant the domain to any client whose UID happens to share that
      // prefix segment (e.g. registering a 1-segment "workload" key would match every SPIFFE
      // workload identity). Operators express grants by creating leaves at the intended depth.
      if (!accumulatedUri.isEmpty()) {
        LOG.info("Registering client URI domain mapping: {} --> {}", accumulatedUri, domainName);
        map.computeIfAbsent(accumulatedUri, k -> new HashSet<>()).add(domainName);
      }
      return;
    }
    for (String child : children) {
      String childUri = accumulatedUri.isEmpty() ? child : accumulatedUri + "/" + child;
      collectClientUris(currentPath + "/" + child, childUri, domainName, map);
    }
  }

  @VisibleForTesting
  void setClientUriToDomainNames(Map<String, Set<String>> mapping) {
    this.clientUriToDomainNames = mapping;
  }

  /**
   * Resolve the set of application domains for a given client URI.
   *
   * Lookup proceeds in two stages:
   * <ol>
   *   <li><b>Exact match</b>: if the URI is registered verbatim in the mapping, its domain set
   *       is returned as-is. URN-style identifiers (e.g. {@code urn:li:servicePrincipal(...)})
   *       contain no {@code '/'} and therefore always resolve here or not at all.</li>
   *   <li><b>Segment-prefix walk-up</b>: if no exact match exists and the URI contains at least
   *       one {@code '/'}, split on {@code '/'} and probe each strictly-shorter left prefix
   *       (anchored at the start, aligned on {@code '/'} boundaries). Domains from every prefix
   *       that is present in the map are unioned into the result. This supports SPIFFE-style ILM
   *       UIDs of the form {@code application/<mp>/<app>[/<tag>]}, where registering
   *       {@code application/<mp>} covers all of its apps and tags without wildcards.</li>
   * </ol>
   * A {@code null} URI yields the empty set.
   */
  @Override
  public Set<String> getDomains(String clientUri) {
    if (clientUri == null) {
      return Collections.emptySet();
    }
    // Snapshot the mapping reference once. parseZNodeMapping reassigns the field on watcher
    // fire; without this snapshot, the exact-match and the prefix walk-up below could read
    // different map references mid-call and return an inconsistent answer.
    Map<String, Set<String>> map = clientUriToDomainNames;
    Set<String> exact = map.get(clientUri);
    if (exact != null) {
      return exact;
    }
    if (clientUri.indexOf('/') < 0) {
      return Collections.emptySet();
    }
    String[] segments = clientUri.split("/");
    Set<String> result = new HashSet<>();
    StringBuilder prefix = new StringBuilder(clientUri.length());
    for (int n = 1; n < segments.length; n++) {
      if (n > 1) {
        prefix.append('/');
      }
      prefix.append(segments[n - 1]);
      Set<String> match = map.get(prefix.toString());
      if (match != null) {
        result.addAll(match);
      }
    }
    return result.isEmpty() ? Collections.emptySet() : result;
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
   * Extends DumbWatcher instead of ServerCnxn because there are some package-private methods in
   * ServerCnxn which cannot be overridden here, such as {@code abstract void setSessionId(long sessionId);}.
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
