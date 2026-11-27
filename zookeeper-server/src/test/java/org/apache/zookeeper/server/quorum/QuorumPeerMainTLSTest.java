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

package org.apache.zookeeper.server.quorum;

import static org.apache.zookeeper.server.quorum.QuorumPeerTestBase.MainThread.UNSET_STATIC_CLIENTPORT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.Security;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.admin.ZooKeeperAdmin;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.common.KeyStoreFileType;
import org.apache.zookeeper.common.X509KeyType;
import org.apache.zookeeper.common.X509TestContext;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.util.ConfigUtils;
import org.apache.zookeeper.test.ClientBase;
import org.apache.zookeeper.test.ReconfigTest;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuorumPeerMainTLSTest extends QuorumPeerTestBase {

    protected static final Logger LOG = LoggerFactory.getLogger(QuorumPeerMainTLSTest.class);
    private static File tempDir;
    private static X509TestContext x509TestContext = null;

    @BeforeAll
    public static void beforeAll() throws Exception {
        Security.addProvider(new BouncyCastleProvider());
        tempDir = ClientBase.createEmptyTestDir();
        x509TestContext = X509TestContext.newBuilder()
            .setTempDir(tempDir)
            .setKeyStoreKeyType(X509KeyType.EC)
            .setTrustStoreKeyType(X509KeyType.EC)
            .build();
    }

    @AfterAll
    public static void afterAll() {
        Security.removeProvider(BouncyCastleProvider.PROVIDER_NAME);
        try {
            FileUtils.deleteDirectory(tempDir);
        } catch (IOException e) {
            // ignore
        }
    }

    // TODO - test reconfig - NIO cnxn factory initially, reconfig to listen on TLS port, should fail coz Netty cnxn factory is needed
    // TODO - equivalent of testReconfigRemoveClientFromStatic, but for secureClientPort
    interface QuorumConfigBuilder {
        String build(int id, String role, int quorumPort, int leaderPort, int clientPort, int secureClientPort);
    }

    static class MaybeSecureServers extends Servers {
        public int[] quorumPorts;
        public int[] leaderPorts;
        public boolean[] isSecureClient;
        public int[] secureClientPorts;
        int numParticipants;
        int numObservers;
        String quorumCfg;

        String otherCfg;

        public MaybeSecureServers(int numParticipants, int numObservers, String otherCfg, QuorumConfigBuilder quorumConfigBuilder) throws IOException {
            this.numParticipants = numParticipants;
            this.numObservers = numObservers;
            this.otherCfg = otherCfg;
            int size = numParticipants + numObservers;

            this.mt = new MainThread[size];
            this.zk = new ZooKeeper[size];
            this.quorumPorts = new int[size];
            this.leaderPorts = new int[size];
            this.clientPorts = new int[size];
            this.adminPorts = new int[size];
            this.secureClientPorts = new int[size];
            this.isSecureClient = new boolean[size];

            StringBuilder quorumCfg = new StringBuilder();

            for (int i = 0; i < size; i++) {
                this.quorumPorts[i] = PortAssignment.unique();
                this.leaderPorts[i] = PortAssignment.unique();
                this.clientPorts[i] = PortAssignment.unique();
                this.adminPorts[i] = PortAssignment.unique();
                this.secureClientPorts[i] = PortAssignment.unique();
                String role = i < numParticipants ? "participant" : "observer";
                String serverEntry = quorumConfigBuilder.build(i, role, this.quorumPorts[i], this.leaderPorts[i], this.clientPorts[i], this.secureClientPorts[i]);
                quorumCfg.append(serverEntry).append("\n");
                if (serverEntry.endsWith("" + this.secureClientPorts[i])) {
                    this.isSecureClient[i] = true;
                }
            }

            this.quorumCfg = quorumCfg.toString();
            for (int i = 0; i < size; i++) {
                this.mt[i] = new MainThread(i, UNSET_STATIC_CLIENTPORT, this.adminPorts[i], null, this.quorumCfg, this.otherCfg, null, true, null);
            }
        }

        public void restartSecureClient(int clientIndex, Watcher watcher) throws IOException, InterruptedException {
            if (zk[clientIndex] != null) {
                zk[clientIndex].close();
            }

            isSecureClient[clientIndex] = true;
            zk[clientIndex] = new ZooKeeper(
                "127.0.0.1:" + secureClientPorts[clientIndex],
                ClientBase.CONNECTION_TIMEOUT,
                watcher, getClientTLSConfigs(x509TestContext));
        }

        public void restartAllServersAndClients(Watcher watcher) throws IOException, InterruptedException {
            for (MainThread t : mt) {
                if (!t.isAlive()) {
                    t.start();
                }
            }
            for (int i = 0; i < zk.length; i++) {
                if (isSecureClient[i]) {
                    restartSecureClient(i, watcher);
                } else {
                    restartClient(i, watcher);
                }
            }
        }
    }

    static Map<String, String> getServerTLSConfigs(X509TestContext x509TestContext) throws IOException {
        Map<String, String> sslConfigs = new HashMap<>();
        sslConfigs.put("ssl.keyStore.location", x509TestContext.getKeyStoreFile(KeyStoreFileType.PEM).getAbsolutePath());
        sslConfigs.put("ssl.trustStore.location", x509TestContext.getTrustStoreFile(KeyStoreFileType.PEM).getAbsolutePath());
        sslConfigs.put("ssl.keyStore.type", "PEM");
        sslConfigs.put("ssl.trustStore.type", "PEM");
        // Netty is required for TLS
        sslConfigs.put("serverCnxnFactory", org.apache.zookeeper.server.NettyServerCnxnFactory.class.getName());
        return sslConfigs;
    }

    static ZKClientConfig getClientTLSConfigs(X509TestContext x509TestContext) throws IOException {
        if (x509TestContext == null) {
            throw new RuntimeException("x509TestContext cannot be null");
        }
        File clientKeyStore = x509TestContext.getKeyStoreFile(KeyStoreFileType.PEM);
        File clientTrustStore = x509TestContext.getTrustStoreFile(KeyStoreFileType.PEM);

        ZKClientConfig zKClientConfig = new ZKClientConfig();
        zKClientConfig.setProperty("zookeeper.client.secure", "true");
        zKClientConfig.setProperty("zookeeper.ssl.keyStore.location", clientKeyStore.getAbsolutePath());
        zKClientConfig.setProperty("zookeeper.ssl.trustStore.location", clientTrustStore.getAbsolutePath());
        zKClientConfig.setProperty("zookeeper.ssl.keyStore.type", "PEM");
        zKClientConfig.setProperty("zookeeper.ssl.trustStore.type", "PEM");
        // only netty supports TLS
        zKClientConfig.setProperty("zookeeper.clientCnxnSocket", org.apache.zookeeper.ClientCnxnSocketNetty.class.getName());
        return zKClientConfig;
    }

    private static String toConfig(Map<String, String> configMap) {
        StringBuilder configBuilder = new StringBuilder();
        for (Map.Entry<String, String> entry : configMap.entrySet()) {
            configBuilder.append(entry.getKey()).append("=").append(entry.getValue()).append("\n");
        }
        return configBuilder.toString();
    }

    private static Map<String, String> reconfigTLSConfigs() throws IOException {
        Map<String, String> configMap = new HashMap<>();
        configMap.put("reconfigEnabled", "true");
        configMap.put("authProvider.x509", "org.apache.zookeeper.server.auth.X509AuthenticationProvider");
        configMap.put("DigestAuthenticationProvider.superDigest", "super:D/InIHSb7yEEbrWz8b9l71RjZJU="/* password is 'test'*/);
        configMap.putAll(getServerTLSConfigs(x509TestContext));
        return configMap;
    }

    private static String serverEntry(MaybeSecureServers servers, int id, String clientSpec) {
        return String.format("server.%d=127.0.0.1:%d:%d:participant%s", id, servers.quorumPorts[id], servers.leaderPorts[id], clientSpec);
    }

    private static void assertSecureOnlyListener(QuorumPeer qp, int secureClientPort) {
        assertNull(qp.cnxnFactory);
        assertNull(qp.getClientAddress());
        assertEquals(-1, qp.getClientPort());
        assertNotNull(qp.secureCnxnFactory);
        assertNotNull(qp.getSecureClientAddress());
        assertEquals(secureClientPort, qp.getSecureClientPort());
        assertEquals(secureClientPort, qp.getSecureClientAddress().getPort());
    }

    private ZooKeeper createSecureClient(int port) throws IOException, InterruptedException {
        ZooKeeper zk = new ZooKeeper("127.0.0.1:" + port, ClientBase.CONNECTION_TIMEOUT, this, getClientTLSConfigs(x509TestContext));
        waitForOne(zk, ZooKeeper.States.CONNECTED);
        return zk;
    }

    /**
     * Starts a single server in replicated mode
     */
    @Test
    public void testTLSQuorumPeers() throws IOException, InterruptedException {
        Map<String, String> configMap = new HashMap<>();
        configMap.put("standaloneEnabled", "false");
        configMap.put("authProvider.x509", "org.apache.zookeeper.server.auth.X509AuthenticationProvider");
        configMap.putAll(getServerTLSConfigs(x509TestContext));

        StringBuilder configBuilder = new StringBuilder();
        for (Map.Entry<String, String> entry : configMap.entrySet()) {
            configBuilder.append(entry.getKey()).append("=").append(entry.getValue()).append("\n");
        }

        MaybeSecureServers maybeSecureServers = new MaybeSecureServers(3, 2, configBuilder.toString(),
            (id, role, quorumPort, leaderPort, clientPort, secureClientPort) -> String.format("server.%d=127.0.0.1:%d:%d:%s;;127.0.0.1:%d", id, quorumPort, leaderPort, role, secureClientPort));

        // wire to "servers" of QuorumPeerTestBase, so it can be destroyed in QuorumPeerTestBase.tearDown()
        servers = maybeSecureServers;

        // start servers and clients
        maybeSecureServers.restartAllServersAndClients(this);

        // wait for clients to connect
        waitForAll(maybeSecureServers, ZooKeeper.States.CONNECTED);

        // Find and log leader
        maybeSecureServers.findLeader();

        QuorumPeer qp0 = maybeSecureServers.mt[0].getQuorumPeer();

        assertNotNull(qp0);

        // verify no listener on client port
        assertNull(qp0.cnxnFactory);
        assertNull(qp0.getClientAddress());
        assertEquals(-1, qp0.getClientPort());

        // verify valid secure client port listener exists
        assertNotNull(qp0.secureCnxnFactory);
        assertNotNull(qp0.getSecureClientAddress());
        assertEquals(maybeSecureServers.secureClientPorts[0], qp0.getSecureClientPort());
        assertEquals(maybeSecureServers.secureClientPorts[0], qp0.getSecureClientAddress().getPort());
    }

    @Test
    public void reconfigFromClientPortToSecureClientPort() throws IOException, InterruptedException, KeeperException {
        Map<String, String> configMap = new HashMap<>();
        configMap.put("reconfigEnabled", "true");
        configMap.put("authProvider.x509", "org.apache.zookeeper.server.auth.X509AuthenticationProvider");
        configMap.put("DigestAuthenticationProvider.superDigest", "super:D/InIHSb7yEEbrWz8b9l71RjZJU="/* password is 'test'*/);
        configMap.putAll(getServerTLSConfigs(x509TestContext));

        StringBuilder configBuilder = new StringBuilder();
        for (Map.Entry<String, String> entry : configMap.entrySet()) {
            configBuilder.append(entry.getKey()).append("=").append(entry.getValue()).append("\n");
        }

        MaybeSecureServers maybeSecureServers = new MaybeSecureServers(3, 0, configBuilder.toString(),
            (id, role, quorumPort, leaderPort, clientPort, secureClientPort) -> String.format("server.%d=127.0.0.1:%d:%d:%s;127.0.0.1:%d", id, quorumPort, leaderPort, role, clientPort));

        servers = maybeSecureServers;

        maybeSecureServers.restartAllServersAndClients(this);

        waitForAll(maybeSecureServers, ZooKeeper.States.CONNECTED);

        ZooKeeperAdmin zkAdmin = new ZooKeeperAdmin("127.0.0.1:" + maybeSecureServers.clientPorts[0], ClientBase.CONNECTION_TIMEOUT, this);
        zkAdmin.addAuthInfo("digest", "super:test".getBytes());

        List<String> joiningServers = new ArrayList<>();
        List<String> leavingServers = new ArrayList<>();

        int reconfigIndex = 1;
        leavingServers.add(Integer.toString(reconfigIndex));
        joiningServers.add(String.format("server.%d=127.0.0.1:%d:%d:%s;;127.0.0.1:%d", reconfigIndex,
            maybeSecureServers.quorumPorts[reconfigIndex], maybeSecureServers.leaderPorts[reconfigIndex],
            "participant", maybeSecureServers.secureClientPorts[reconfigIndex]));

        ReconfigTest.reconfig(zkAdmin, null, leavingServers, null, -1);
        LOG.info("Reconfig REMOVE done with leavingServers={}!", leavingServers);
        ReconfigTest.testServerHasConfig(maybeSecureServers.zk[0], null, leavingServers);

        ReconfigTest.reconfig(zkAdmin, joiningServers, null, null, -1);
        LOG.info("Reconfig ADD done with joiningServers={}!", joiningServers);

        ReconfigTest.testServerHasConfig(maybeSecureServers.zk[0], joiningServers, null);

        assertTrue(ClientBase.waitForServerDown("127.0.0.1:" + maybeSecureServers.clientPorts[reconfigIndex], 5000, false));

        maybeSecureServers.restartSecureClient(reconfigIndex, this);
        waitForOne(maybeSecureServers.zk[reconfigIndex], ZooKeeper.States.CONNECTED);
        ReconfigTest.testNormalOperation(maybeSecureServers.zk[0], maybeSecureServers.zk[reconfigIndex]);
    }

    /**
     * A TLS-only quorum publishes the secure-only form in its dynamic config,
     * persists it, and comes back secure-only after a restart from that file.
     */
    @Test
    public void testTLSOnlyDynamicConfigIsServedPersistedAndReloaded() throws Exception {
        MaybeSecureServers maybeSecureServers = new MaybeSecureServers(3, 0, toConfig(reconfigTLSConfigs()),
            (id, role, quorumPort, leaderPort, clientPort, secureClientPort) -> String.format("server.%d=127.0.0.1:%d:%d:%s;;127.0.0.1:%d", id, quorumPort, leaderPort, role, secureClientPort));
        servers = maybeSecureServers;
        maybeSecureServers.restartAllServersAndClients(this);
        waitForAll(maybeSecureServers, ZooKeeper.States.CONNECTED);

        List<String> expectedEntries = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            assertSecureOnlyListener(maybeSecureServers.mt[i].getQuorumPeer(), maybeSecureServers.secureClientPorts[i]);
            assertFalse(ClientBase.waitForServerUp("127.0.0.1:" + maybeSecureServers.secureClientPorts[i], 1000, false));
            expectedEntries.add(serverEntry(maybeSecureServers, i, ";;127.0.0.1:" + maybeSecureServers.secureClientPorts[i]));
        }
        ReconfigTest.testNormalOperation(maybeSecureServers.zk[0], maybeSecureServers.zk[1]);

        String config = ReconfigTest.testServerHasConfig(maybeSecureServers.zk[2], expectedEntries, null);
        // client-side connect-string extraction only reports plaintext client addresses
        assertEquals(config.substring(config.indexOf("version=") + "version=".length()).trim() + " ,,",
            ConfigUtils.getClientConfigStr(config));

        Properties staticCfg = ReconfigLegacyTest.readPropertiesFromFile(maybeSecureServers.mt[2].confFile);
        assertFalse(staticCfg.containsKey("clientPort"));
        assertFalse(staticCfg.containsKey("secureClientPort"));
        File dynamicFile = new File(staticCfg.getProperty("dynamicConfigFile"));
        String persisted = new String(Files.readAllBytes(dynamicFile.toPath()), StandardCharsets.UTF_8);
        for (String entry : expectedEntries) {
            assertTrue(persisted.contains(entry), persisted);
        }

        maybeSecureServers.mt[2].shutdown();
        maybeSecureServers.zk[2].close();
        maybeSecureServers.mt[2].start();
        maybeSecureServers.restartSecureClient(2, this);
        waitForOne(maybeSecureServers.zk[2], ZooKeeper.States.CONNECTED);
        assertSecureOnlyListener(maybeSecureServers.mt[2].getQuorumPeer(), maybeSecureServers.secureClientPorts[2]);
        ReconfigTest.testNormalOperation(maybeSecureServers.zk[0], maybeSecureServers.zk[2]);
    }

    /**
     * Servers configured with both a plaintext and a secure client address serve both.
     */
    @Test
    public void testDualClientListeners() throws Exception {
        MaybeSecureServers maybeSecureServers = new MaybeSecureServers(3, 0, toConfig(reconfigTLSConfigs()),
            (id, role, quorumPort, leaderPort, clientPort, secureClientPort) -> String.format("server.%d=127.0.0.1:%d:%d:%s;127.0.0.1:%d;127.0.0.1:%d", id, quorumPort, leaderPort, role, clientPort, secureClientPort));
        servers = maybeSecureServers;
        maybeSecureServers.restartAllServersAndClients(this);
        waitForAll(maybeSecureServers, ZooKeeper.States.CONNECTED);

        QuorumPeer qp0 = maybeSecureServers.mt[0].getQuorumPeer();
        assertEquals(maybeSecureServers.clientPorts[0], qp0.getClientPort());
        assertEquals(maybeSecureServers.secureClientPorts[0], qp0.getSecureClientPort());

        ZooKeeper plaintext = ClientBase.createZKClient("127.0.0.1:" + maybeSecureServers.clientPorts[0]);
        try {
            ReconfigTest.testNormalOperation(plaintext, maybeSecureServers.zk[0]);
            ReconfigTest.testServerHasConfig(plaintext, Collections.singletonList(serverEntry(maybeSecureServers, 0,
                ";127.0.0.1:" + maybeSecureServers.clientPorts[0] + ";127.0.0.1:" + maybeSecureServers.secureClientPorts[0])), null);
        } finally {
            plaintext.close();
        }
    }

    /**
     * Reconfigures the client addresses of a running member in place and verifies
     * the listeners that remain or are moved keep serving clients.
     */
    @Test
    public void testInPlaceReconfigOfClientAddresses() throws Exception {
        MaybeSecureServers maybeSecureServers = new MaybeSecureServers(3, 0, toConfig(reconfigTLSConfigs()),
            (id, role, quorumPort, leaderPort, clientPort, secureClientPort) -> String.format("server.%d=127.0.0.1:%d:%d:%s;127.0.0.1:%d;127.0.0.1:%d", id, quorumPort, leaderPort, role, clientPort, secureClientPort));
        servers = maybeSecureServers;
        maybeSecureServers.restartAllServersAndClients(this);
        waitForAll(maybeSecureServers, ZooKeeper.States.CONNECTED);

        ZooKeeperAdmin zkAdmin = new ZooKeeperAdmin("127.0.0.1:" + maybeSecureServers.secureClientPorts[0],
            ClientBase.CONNECTION_TIMEOUT, this, getClientTLSConfigs(x509TestContext));
        try {
            zkAdmin.addAuthInfo("digest", "super:test".getBytes());
            int target = 2;
            QuorumPeer qp = maybeSecureServers.mt[target].getQuorumPeer();

            // A joiner must have at least one client address.
            String noClientAddress = String.format("server.%d=127.0.0.1:%d:%d:participant", 5, PortAssignment.unique(), PortAssignment.unique());
            assertThrows(KeeperException.BadArgumentsException.class,
                () -> zkAdmin.reconfigure(Collections.singletonList(noClientAddress), null, null, -1, new Stat()));

            // drop the plaintext listener
            String tlsOnly = serverEntry(maybeSecureServers, target, ";;127.0.0.1:" + maybeSecureServers.secureClientPorts[target]);
            ReconfigTest.reconfig(zkAdmin, Collections.singletonList(tlsOnly), null, null, -1);
            ReconfigTest.testServerHasConfig(maybeSecureServers.zk[0], Collections.singletonList(tlsOnly), null);
            assertTrue(ClientBase.waitForServerDown("127.0.0.1:" + maybeSecureServers.clientPorts[target], ClientBase.CONNECTION_TIMEOUT, false));
            assertSecureOnlyListener(qp, maybeSecureServers.secureClientPorts[target]);
            ReconfigTest.testNormalOperation(maybeSecureServers.zk[0], maybeSecureServers.zk[target]);

            // move the secure listener
            int movedSecurePort = PortAssignment.unique();
            String moved = serverEntry(maybeSecureServers, target, ";;127.0.0.1:" + movedSecurePort);
            ReconfigTest.reconfig(zkAdmin, Collections.singletonList(moved), null, null, -1);
            ReconfigTest.testServerHasConfig(maybeSecureServers.zk[0], Collections.singletonList(moved), null);
            assertSecureOnlyListener(qp, movedSecurePort);
            ZooKeeper movedClient = createSecureClient(movedSecurePort);
            try {
                ReconfigTest.testNormalOperation(maybeSecureServers.zk[0], movedClient);
            } finally {
                movedClient.close();
            }
        } finally {
            zkAdmin.close();
        }
    }

}
