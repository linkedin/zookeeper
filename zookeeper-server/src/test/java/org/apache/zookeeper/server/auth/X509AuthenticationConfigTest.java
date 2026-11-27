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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import java.util.Properties;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.test.ClientBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class X509AuthenticationConfigTest extends ZKTestCase {
    @AfterEach
    public void tearDown() {
        System.clearProperty(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_TYPE);
        System.clearProperty(X509AuthenticationConfig.ENFORCE_DEDICATED_DOMAIN);
        System.clearProperty(X509AuthenticationConfig.ZOOKEEPER_ZNODEGROUPACL_SUPERUSER_ID);
        X509AuthenticationConfig.reset();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testConfigurationFilePrefixes(boolean fullyQualified) throws Exception {
        X509AuthenticationConfig.reset();
        String prefix = fullyQualified ? "zookeeper." : "";
        Properties properties = new Properties();
        properties.setProperty("dataDir", ClientBase.createTmpDir().getAbsolutePath());
        properties.setProperty(prefix + "ssl.x509.clientCertIdType", "SAN");
        properties.setProperty(prefix + "X509ZNodeGroupAclProvider.enforceDedicatedDomain", "false");
        new QuorumPeerConfig().parseProperties(properties);
        assertEquals("SAN", X509AuthenticationConfig.getInstance().getClientCertIdType());
        assertFalse(X509AuthenticationConfig.getInstance().isEnforceDedicatedDomainEnabled());
        assertFalse(X509AuthenticationConfig.getInstance().isX509ClientIdAsAclEnabled());
    }
}
