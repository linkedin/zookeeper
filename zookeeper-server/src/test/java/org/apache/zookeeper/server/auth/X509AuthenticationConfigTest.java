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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.test.ClientBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
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

    @Test
    public void testConcurrentLazyPublication() throws Exception {
        X509AuthenticationConfig.reset();
        System.setProperty(X509AuthenticationConfig.ZOOKEEPER_ZNODEGROUPACL_SUPERUSER_ID, "one,two");
        // A stress test alone cannot prove the Java memory-model requirement for double-checked locking.
        assertTrue(Modifier.isVolatile(X509AuthenticationConfig.class.getDeclaredField("instance").getModifiers()));
        for (String field : Arrays.asList("znodeGroupAclSuperUserIds", "crossDomainAccessDomains", "openReadAccessPathPrefixes")) {
            assertTrue(Modifier.isVolatile(X509AuthenticationConfig.class.getDeclaredField(field).getModifiers()), field);
        }
        ExecutorService executor = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<X509AuthenticationConfig>> futures = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                futures.add(executor.submit(() -> {
                    assertTrue(start.await(10, TimeUnit.SECONDS));
                    X509AuthenticationConfig config = X509AuthenticationConfig.getInstance();
                    assertEquals(new HashSet<>(Arrays.asList("one", "two")), config.getZnodeGroupAclSuperUserIds());
                    assertTrue(config.getZnodeGroupAclCrossDomainAccessDomains().isEmpty());
                    assertTrue(config.getZnodeGroupAclOpenReadAccessPathPrefixes().isEmpty());
                    return config;
                }));
            }
            start.countDown();
            X509AuthenticationConfig singleton = futures.get(0).get(10, TimeUnit.SECONDS);
            for (Future<X509AuthenticationConfig> future : futures) {
                assertSame(singleton, future.get(10, TimeUnit.SECONDS));
            }
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
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
