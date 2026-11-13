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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.File;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.common.ClientX509Util;
import org.apache.zookeeper.common.X509TestHelpers;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.NettyServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.auth.ProviderRegistry;
import org.apache.zookeeper.server.auth.X509AuthenticationConfig;
import org.apache.zookeeper.test.ClientBase;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Real CA-signed certificates, Netty TLS, clients and persistent mapping watches. */
public class X509GroupAclIntegrationTest extends ZKTestCase {
    private static final String ROOT = "/zookeeper/uri-domain-map";
    private static final String PASSWORD = "testpass";
    private final Map<String, String> savedProperties = new HashMap<>();
    private final List<ZooKeeper> clients = new ArrayList<>();
    private final Map<String, Path> keyStores = new HashMap<>();
    private final Map<ZooKeeper, LinkedBlockingQueue<Watcher.Event.KeeperState>> states = new HashMap<>();
    private ClientX509Util x509;
    private ZooKeeperServer server;
    private NIOServerCnxnFactory adminFactory;
    private NettyServerCnxnFactory tlsFactory;
    private ZooKeeper admin;
    private String tlsAddress;
    private static boolean addedProvider;

    @BeforeAll
    public static void registerProvider() {
        if (Security.getProvider("BC") == null) {
            Security.addProvider(new BouncyCastleProvider());
            addedProvider = true;
        }
    }

    @AfterAll
    public static void removeProvider() {
        if (addedProvider) {
            Security.removeProvider("BC");
        }
    }

    private void property(String key, String value) {
        if (!savedProperties.containsKey(key)) {
            savedProperties.put(key, System.getProperty(key));
        }
        System.setProperty(key, value);
    }

    private void start(boolean unified, boolean automatic) throws Exception {
        X509AuthenticationConfig.reset();
        File directory = ClientBase.createTmpDir();
        KeyPair caKey = X509TestHelpers.generateRSAKeyPair();
        X509Certificate ca = X509TestHelpers.newSelfSignedCACert(new X500Name("CN=P03 test CA"), caKey, 86400000);
        Path trustStore = directory.toPath().resolve("trust.jks");
        KeyStore trust = KeyStore.getInstance("JKS");
        trust.load(null, PASSWORD.toCharArray());
        trust.setCertificateEntry("ca", ca);
        try (OutputStream out = Files.newOutputStream(trustStore)) {
            trust.store(out, PASSWORD.toCharArray());
        }
        for (String name : new String[]{"server", "alice", "bob", "super"}) {
            KeyPair key = X509TestHelpers.generateRSAKeyPair();
            GeneralNames sans = new GeneralNames(new GeneralName[]{
                new GeneralName(GeneralName.dNSName, "localhost"),
                new GeneralName(GeneralName.uniformResourceIdentifier, "urn:example:" + name)});
            X509Certificate cert = X509TestHelpers.newCert(
                ca, caKey, new X500Name("CN=" + name), key.getPublic(), 86400000, sans);
            KeyStore store = KeyStore.getInstance("JKS");
            store.load(null, PASSWORD.toCharArray());
            store.setKeyEntry(name, key.getPrivate(), PASSWORD.toCharArray(), new Certificate[]{cert, ca});
            Path path = directory.toPath().resolve(name + ".jks");
            try (OutputStream out = Files.newOutputStream(path)) {
                store.store(out, PASSWORD.toCharArray());
            }
            keyStores.put(name, path);
        }
        x509 = new ClientX509Util();
        property(x509.getSslKeystoreLocationProperty(), keyStores.get("server").toString());
        property(x509.getSslKeystorePasswdProperty(), PASSWORD);
        property(x509.getSslKeystoreTypeProperty(), "JKS");
        property(x509.getSslTruststoreLocationProperty(), trustStore.toString());
        property(x509.getSslTruststorePasswdProperty(), PASSWORD);
        property(x509.getSslTruststoreTypeProperty(), "JKS");
        property(x509.getSslHostnameVerificationEnabledProperty(), "true");
        property("zookeeper.authProvider.x509", X509ZNodeGroupAclProvider.class.getName());
        property(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_TYPE, "SAN");
        property(X509AuthenticationConfig.SSL_X509_CLIENT_CERT_ID_SAN_MATCH_TYPE, "6");
        property(X509AuthenticationConfig.ZOOKEEPER_ZNODEGROUPACL_SUPERUSER_ID, "urn:example:super");
        if (automatic) {
            property(X509AuthenticationConfig.SET_X509_CLIENT_ID_AS_ACL, "true");
        }
        property(NettyServerCnxnFactory.PORT_UNIFICATION_KEY, Boolean.toString(unified));
        ProviderRegistry.reset();
        server = new ZooKeeperServer(directory, directory, 3000);
        adminFactory = new NIOServerCnxnFactory();
        adminFactory.configure(new InetSocketAddress("127.0.0.1", PortAssignment.unique()), 20, -1, false);
        adminFactory.startup(server);
        admin = ClientBase.createZKClient("127.0.0.1:" + adminFactory.getLocalPort());
        property(ServerCnxnFactory.ZOOKEEPER_SERVER_CNXN_FACTORY, NettyServerCnxnFactory.class.getName());
        tlsFactory = (NettyServerCnxnFactory) ServerCnxnFactory.createFactory();
        tlsFactory.configure(new InetSocketAddress("127.0.0.1", PortAssignment.unique()), 20, -1, !unified);
        tlsFactory.startup(server, false);
        tlsAddress = "localhost:" + tlsFactory.getLocalPort();
    }

    private ZooKeeper connect(String name, Watcher watcher) throws Exception {
        ZKClientConfig config = new ZKClientConfig();
        config.setProperty(ZKClientConfig.SECURE_CLIENT, "true");
        config.setProperty(ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET, "org.apache.zookeeper.ClientCnxnSocketNetty");
        config.setProperty(x509.getSslKeystoreLocationProperty(), keyStores.get(name).toString());
        LinkedBlockingQueue<Watcher.Event.KeeperState> events = new LinkedBlockingQueue<>();
        ZooKeeper client = new ZooKeeper(tlsAddress, 15000, event -> {
            if (event.getType() == Watcher.Event.EventType.None) {
                events.add(event.getState());
            }
            watcher.process(event);
        }, config);
        clients.add(client);
        states.put(client, events);
        return client;
    }

    private ZooKeeper connect(String name) throws Exception {
        CountDownLatch connected = new CountDownLatch(1);
        ZooKeeper client = connect(name, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connected.countDown();
            }
        });
        assertTrue(connected.await(10, TimeUnit.SECONDS), "TLS client did not connect: " + name);
        return client;
    }

    private void map(String domain, String name) throws Exception {
        for (String path : new String[]{ROOT, ROOT + "/" + domain, ROOT + "/" + domain + "/urn:example:" + name}) {
            if (admin.exists(path, false) == null) {
                admin.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        }
    }

    private List<ACL> acl(String id) {
        return Collections.singletonList(new ACL(ZooDefs.Perms.ALL, new Id("x509", id)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testMappingLifecycleAndReadAuthorization(boolean unified) throws Exception {
        start(unified, true);
        assertNull(admin.exists(ROOT, false));
        ZooKeeper alice = connect("alice");
        assertNull(admin.exists(ROOT, false), "authentication must not create the mapping root");
        alice.create("/unmapped", new byte[]{1}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        assertEquals(acl("urn:example:alice"), alice.getACL("/unmapped", null));
        map("teamA", "alice");
        map("teamB", "bob");
        alice.create("/owned", new byte[]{2}, ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
        assertEquals(acl("teamA"), alice.getACL("/owned", null));
        ZooKeeper bob = connect("bob");
        assertThrows(KeeperException.NoAuthException.class, () -> bob.getData("/owned", true, null));
        assertThrows(KeeperException.NoAuthException.class, () -> bob.getChildren("/owned", true));
        assertThrows(KeeperException.NoAuthException.class, () -> bob.exists("/owned", true));
        assertThrows(KeeperException.NoAuthException.class, () -> bob.setData("/owned", new byte[]{3}, -1));
        alice.setACL("/owned", ZooDefs.Ids.OPEN_ACL_UNSAFE, -1);
        assertEquals(acl("teamA"), alice.getACL("/owned", null), "automatic ACL must not become world writable");

        ZooKeeper superUser = connect("super");
        LinkedBlockingQueue<WatchedEvent> events = new LinkedBlockingQueue<>();
        bob.addWatch("/", event -> {
            if (event.getType() != Watcher.Event.EventType.None) {
                events.add(event);
            }
        }, AddWatchMode.PERSISTENT_RECURSIVE);
        superUser.setData("/owned", new byte[]{4}, -1);
        superUser.create("/barrier", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        WatchedEvent event = events.poll(10, TimeUnit.SECONDS);
        assertNotNull(event);
        assertEquals("/barrier", event.getPath(), "a denied READ watch must not disclose /owned");
        assertNull(events.poll(200, TimeUnit.MILLISECONDS));

        map("teamA", "bob");
        assertArrayEquals(new byte[]{4}, bob.getData("/owned", false, null));
        admin.delete(ROOT + "/teamA/urn:example:bob", -1);
        assertThrows(KeeperException.NoAuthException.class, () -> bob.getData("/owned", false, null));
        long sessionId = alice.getSessionId();
        states.values().forEach(LinkedBlockingQueue::clear);
        tlsFactory.closeAll(ServerCnxn.DisconnectReason.CHANNEL_DISCONNECTED);
        for (ZooKeeper client : clients) {
            assertEquals(Watcher.Event.KeeperState.Disconnected, states.get(client).poll(10, TimeUnit.SECONDS));
            assertEquals(Watcher.Event.KeeperState.SyncConnected, states.get(client).poll(10, TimeUnit.SECONDS));
        }
        assertEquals(sessionId, alice.getSessionId());
        assertArrayEquals(new byte[]{4}, alice.getData("/owned", false, null));
        assertThrows(KeeperException.NoAuthException.class, () -> bob.getData("/owned", false, null));

        ZKUtil.deleteRecursive(admin, ROOT);
        assertThrows(KeeperException.NoAuthException.class, () -> alice.getData("/owned", false, null));
        map("teamA", "alice");
        assertArrayEquals(new byte[]{4}, alice.getData("/owned", false, null));
        superUser.setACL("/owned", ZooDefs.Ids.OPEN_ACL_UNSAFE, -1);
        assertArrayEquals(new byte[]{4}, bob.getData("/owned", false, null));
    }

    @ParameterizedTest
    @CsvSource({"false,true", "true,true", "false,false", "true,false"})
    public void testDedicatedDomainFiltering(boolean unified, boolean enforce) throws Exception {
        property(X509AuthenticationConfig.DEDICATED_DOMAIN, "teamA");
        property(X509AuthenticationConfig.ENFORCE_DEDICATED_DOMAIN, Boolean.toString(enforce));
        start(unified, false);
        map("teamA", "alice");
        ZooKeeper alice = connect("alice");
        alice.create("/open", new byte[]{1}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        ZooKeeper superUser = connect("super");
        assertArrayEquals(new byte[]{1}, superUser.getData("/open", false, null));
        CountDownLatch connected = new CountDownLatch(1);
        ZooKeeper bob = connect("bob", event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connected.countDown();
            }
        });
        if (enforce) {
            assertFalse(connected.await(2, TimeUnit.SECONDS), "wrong-domain client must be disconnected");
        } else {
            assertTrue(connected.await(10, TimeUnit.SECONDS));
            assertArrayEquals(new byte[]{1}, bob.getData("/open", false, null));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testAutomaticAclDisabledByDefault(boolean unified) throws Exception {
        start(unified, false);
        map("teamA", "alice");
        ZooKeeper alice = connect("alice");
        alice.create("/open", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        assertEquals(ZooDefs.Ids.OPEN_ACL_UNSAFE, alice.getACL("/open", null));
        alice.create("/creator", null, ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
        assertEquals(acl("teamA"), alice.getACL("/creator", null));
    }

    @AfterEach
    public void tearDown() throws Exception {
        try {
            for (ZooKeeper client : clients) {
                client.close();
            }
            if (admin != null) {
                admin.close();
            }
            if (tlsFactory != null) {
                tlsFactory.shutdown();
            }
            if (adminFactory != null) {
                adminFactory.shutdown();
            }
            if (server != null) {
                server.shutdown();
                server.getZKDatabase().close();
            }
        } finally {
            if (x509 != null) {
                x509.close();
            }
            savedProperties.forEach((key, value) -> {
                if (value == null) {
                    System.clearProperty(key);
                } else {
                    System.setProperty(key, value);
                }
            });
            X509AuthenticationConfig.reset();
            ProviderRegistry.reset();
        }
    }
}
