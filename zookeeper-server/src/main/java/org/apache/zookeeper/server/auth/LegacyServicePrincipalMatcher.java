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

import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.auth.X509AuthenticationUtil.CertificateType;
import org.apache.zookeeper.server.auth.X509AuthenticationUtil.ClientIdentity;

/**
 * Compatibility matching shared by X509 ACLs, URI-domain mappings and opt-in superuser selection.
 */
public final class LegacyServicePrincipalMatcher {
    private static final Pattern LEGACY_SERVICE_PRINCIPAL_PATTERN =
        Pattern.compile("^(?:urn:li:)?servicePrincipal\\(([^();/]+)(?:\\)|;[^()/]*\\))?$");
    private static final Pattern BARE_APPLICATION_PATTERN = Pattern.compile("^[A-Za-z0-9][A-Za-z0-9._-]*$");
    private static final Pattern SPIFFE_APPLICATION_PATTERN =
        Pattern.compile("^application/[^/]+/([^/]+)(?:/[^/]+)?$");

    private LegacyServicePrincipalMatcher() {
    }

    /**
     * Return the configured ID so ACL preparation continues to recognize an explicit superuser.
     * Exact matches take precedence; otherwise choose a stable marker among compatible IDs.
     */
    public static Optional<String> findMatchingSuperUserId(ClientIdentity identity, Set<String> configuredIds) {
        if (configuredIds.contains(identity.getId())) {
            return Optional.of(identity.getId());
        }
        if (!X509AuthenticationConfig.getInstance().isLegacySuperUserCompatibilityEnabled()) {
            return Optional.empty();
        }
        return configuredIds.stream()
            .filter(id -> matches(identity.getCertificateType(), identity.getId(), id))
            .sorted()
            .findFirst();
    }

    public static boolean matches(CertificateType certificateType, String clientId, String legacyId) {
        String applicationName = getApplicationName(legacyId);
        if (applicationName == null || clientId == null) {
            return false;
        }
        if (certificateType == CertificateType.SPIFFE_V1_WL) {
            return applicationName.equals(clientId);
        }
        if (certificateType == CertificateType.SPIFFE_V1_WORKLOAD || certificateType == CertificateType.SPIFFE_V2) {
            Matcher matcher = SPIFFE_APPLICATION_PATTERN.matcher(clientId);
            return matcher.matches() && applicationName.equals(matcher.group(1));
        }
        return false;
    }

    public static boolean matchesAuthenticatedClient(ServerCnxn cnxn, String authenticatedId, String aclId) {
        if (cnxn == null) {
            return false;
        }
        // Bind the candidate AuthInfo ID to the authenticated certificate identity, not a mapped domain.
        ClientIdentity identity = cnxn.getX509ClientIdentity();
        return identity != null && identity.getId().equals(authenticatedId)
            && matches(identity.getCertificateType(), authenticatedId, aclId);
    }

    private static String getApplicationName(String legacyId) {
        if (legacyId == null) {
            return null;
        }
        Matcher matcher = LEGACY_SERVICE_PRINCIPAL_PATTERN.matcher(legacyId);
        if (matcher.matches()) {
            return matcher.group(1);
        }
        return BARE_APPLICATION_PATTERN.matcher(legacyId).matches() ? legacyId : null;
    }
}
