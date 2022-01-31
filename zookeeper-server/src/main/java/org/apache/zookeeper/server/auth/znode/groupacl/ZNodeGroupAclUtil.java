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

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Util class for ZNode Group ACL. Contains util methods and constants.
 */
public class ZNodeGroupAclUtil {

  /**
   * Property key values (JVM configs) for ZNode group ACL features
   */
  public static final String ZNODE_GROUP_ACL_CONFIG_PREFIX = "zookeeper.ssl.znodeGroupAcl.";
  // Enables/disables whether znodes created by auth'ed clients
  // should have ACL fields populated with the client Id given by the authentication provider.
  // Has the same effect as the ZK client using ZooDefs.Ids.CREATOR_ALL_ACL.
  public static final String SET_X509_CLIENT_ID_AS_ACL =
      ZNODE_GROUP_ACL_CONFIG_PREFIX + "setX509ClientIdAsAcl";
  // A list of domain names that will have super user privilege, separated by ","
  public static final String SUPER_USER_DOMAIN_NAME =
      ZNODE_GROUP_ACL_CONFIG_PREFIX + "superUserDomainName";
  // A list of znode path prefixes, separated by ","
  // Znode whose path starts with the defined path prefix would have open read access
  // Meaning the znode will have (world:anyone, r) ACL
  public static final String OPEN_READ_ACCESS_PATH_PREFIX =
      ZNODE_GROUP_ACL_CONFIG_PREFIX + "openReadAccessPathPrefix";
  private static Set<String> openReadAccessPathPrefixes = null;
  private static Set<String> superUserDomainNames = null;

  /**
   * Get open read access path prefixes from config
   * @return A set of path prefixes
   */
  public static Set<String> getOpenReadAccessPathPrefixes() {
    if (openReadAccessPathPrefixes == null) {
      String openReadAccessPathPrefixesStr = System.getProperty(OPEN_READ_ACCESS_PATH_PREFIX);
      if (openReadAccessPathPrefixesStr == null || openReadAccessPathPrefixesStr.isEmpty()) {
        return Collections.emptySet();
      }
      openReadAccessPathPrefixes =
          Arrays.stream(openReadAccessPathPrefixesStr.split(",")).filter(str -> str.length() > 0)
              .collect(Collectors.toSet());
    }
    return openReadAccessPathPrefixes;
  }

  /**
   * Get the domain names that are mapped to super user access privilege
   * @return A set of domain names
   */
  public static Set<String> getSuperUserDomainNames() {
    if (superUserDomainNames == null) {
      String superUserDomainNameStr = System.getProperty(ZNodeGroupAclUtil.SUPER_USER_DOMAIN_NAME);
      if (superUserDomainNameStr == null || superUserDomainNameStr.isEmpty()) {
        return Collections.emptySet();
      }
      superUserDomainNames =
          Arrays.stream(superUserDomainNameStr.split(",")).filter(str -> str.length() > 0)
              .collect(Collectors.toSet());
    }
    return superUserDomainNames;
  }
}
