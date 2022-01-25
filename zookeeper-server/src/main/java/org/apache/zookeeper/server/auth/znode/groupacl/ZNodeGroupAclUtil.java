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
  public static final String SUPER_USER_DOMAIN_NAME =
      ZNODE_GROUP_ACL_CONFIG_PREFIX + "superUserDomainName";
  public static final String OPEN_READ_ACCESS_PATH_PREFIX =
      ZNODE_GROUP_ACL_CONFIG_PREFIX + "openReadAccessPathPrefix";
  public static final String CONFIG_VALUE_LIST_DELIMITER =
      ZNODE_GROUP_ACL_CONFIG_PREFIX + "configValueListDelimiter";

  public static String[] getOpenReadAccessPathPrefixes() {
    String configValListDelimiter = System.getProperty(CONFIG_VALUE_LIST_DELIMITER, ",");
    String openReadAccessPathPrefixesStr = System.getProperty(OPEN_READ_ACCESS_PATH_PREFIX, "");
    return openReadAccessPathPrefixesStr.split(configValListDelimiter);
  }

  public static Set<String> getSuperUserDomainNames() {
    String configValListDelimiter = System.getProperty(CONFIG_VALUE_LIST_DELIMITER, ",");
    String superUserDomainNameStr =
        System.getProperty(ZNodeGroupAclUtil.SUPER_USER_DOMAIN_NAME, "");
    return Arrays.stream(superUserDomainNameStr.split(configValListDelimiter))
        .filter(str -> str.length() > 0).collect(Collectors.toSet());
  }
}
