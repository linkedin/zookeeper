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

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.auth.X509AuthenticationUtil.CertificateType;
import org.apache.zookeeper.server.auth.X509AuthenticationUtil.ClientIdentity;

/**
 * Compatibility matching shared by direct X509 ACLs and URI-domain mappings.
 */
public final class LegacyServicePrincipalMatcher {
    private static final Pattern LEGACY_SERVICE_PRINCIPAL_PATTERN =
        Pattern.compile("^(?:urn:li:)?servicePrincipal\\(([^();/]+)(?:\\)|;[^()/]*\\))?$");

    private LegacyServicePrincipalMatcher() {
    }

    public static boolean matches(CertificateType certificateType, String clientId, String legacyPrincipal) {
        return certificateType == CertificateType.SPIFFE_V1_WL
            && clientId != null && clientId.equals(getApplicationName(legacyPrincipal));
    }

    public static boolean matchesAuthenticatedClient(ServerCnxn cnxn, String authenticatedId, String aclId) {
        boolean workloadToLegacy = authenticatedId != null && authenticatedId.equals(getApplicationName(aclId));
        boolean legacyToWorkload = aclId != null && aclId.equals(getApplicationName(authenticatedId));
        if (cnxn == null || (!workloadToLegacy && !legacyToWorkload)) {
            return false;
        }
        // Bind the candidate AuthInfo ID to the authenticated certificate identity, not a mapped domain.
        ClientIdentity identity = cnxn.getX509ClientIdentity();
        return identity != null && identity.getId().equals(authenticatedId)
            && ((identity.getCertificateType() == CertificateType.SPIFFE_V1_WL && workloadToLegacy)
                || (identity.getCertificateType() == CertificateType.LEGACY_SAN && legacyToWorkload));
    }

    private static String getApplicationName(String legacyPrincipal) {
        if (legacyPrincipal == null) {
            return null;
        }
        Matcher matcher = LEGACY_SERVICE_PRINCIPAL_PATTERN.matcher(legacyPrincipal);
        return matcher.matches() ? matcher.group(1) : null;
    }
}
