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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.X509AuthenticationUtil.CertificateType;
import org.apache.zookeeper.server.auth.X509AuthenticationUtil.ClientIdentity;

/**
 * Transports authenticated identity context between quorum peers, separately from ordinary
 * AuthInfo. The reserved entry is never a client authentication scheme or a stored ACL.
 */
public final class X509QuorumAuthInfo {
    public static final String AUTH_SCHEME = "zookeeper-internal-x509";
    private static final String VERSION = "1";

    private final List<Id> authInfo;
    private final ClientIdentity clientIdentity;

    private X509QuorumAuthInfo(List<Id> authInfo, ClientIdentity clientIdentity) {
        this.authInfo = authInfo;
        this.clientIdentity = clientIdentity;
    }

    public List<Id> getAuthInfo() {
        return authInfo;
    }

    public ClientIdentity getClientIdentity() {
        return clientIdentity;
    }

    public static List<Id> encode(List<Id> authInfo, ClientIdentity identity) throws IOException {
        List<Id> encoded = authInfo == null ? new ArrayList<>() : new ArrayList<>(authInfo);
        for (Id id : encoded) {
            if (AUTH_SCHEME.equals(id.getScheme())) {
                throw new IOException("Reserved X509 quorum metadata cannot be ordinary AuthInfo");
            }
        }
        if (identity == null) {
            return authInfo;
        }
        encoded.add(new Id(AUTH_SCHEME,
            VERSION + ":" + identity.getCertificateType().name() + ":" + identity.getId()));
        return encoded;
    }

    public static X509QuorumAuthInfo decode(List<Id> encoded) throws IOException {
        if (encoded == null) {
            return new X509QuorumAuthInfo(null, null);
        }
        List<Id> authInfo = new ArrayList<>();
        ClientIdentity identity = null;
        for (Id id : encoded) {
            if (!AUTH_SCHEME.equals(id.getScheme())) {
                authInfo.add(id);
                continue;
            }
            if (identity != null) {
                throw new IOException("Duplicate X509 quorum identity metadata");
            }
            String value = id.getId();
            String[] fields = value == null ? new String[0] : value.split(":", 3);
            if (fields.length != 3 || !VERSION.equals(fields[0])) {
                throw new IOException("Invalid X509 quorum identity metadata version or format");
            }
            try {
                identity = new ClientIdentity(CertificateType.valueOf(fields[1]), fields[2]);
            } catch (IllegalArgumentException e) {
                throw new IOException("Invalid X509 quorum certificate type", e);
            }
        }
        return new X509QuorumAuthInfo(authInfo, identity);
    }
}
