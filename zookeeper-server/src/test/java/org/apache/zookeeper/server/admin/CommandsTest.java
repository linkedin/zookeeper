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

package org.apache.zookeeper.server.admin;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Quotas;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.cli.SetQuotaCommand;
import org.apache.zookeeper.metrics.MetricsUtils;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerStats;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.quorum.BufferStats;
import org.apache.zookeeper.test.ClientBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CommandsTest extends ClientBase {

    private static final String QUOTA_ALLOWLIST = "zookeeper.quotaStats.allowedNamespaces";
    private String originalQuotaAllowlist;

    @Before
    public void saveQuotaAllowlist() {
        originalQuotaAllowlist = System.getProperty(QUOTA_ALLOWLIST);
        System.clearProperty(QUOTA_ALLOWLIST);
    }

    @After
    public void restoreQuotaAllowlist() {
        if (originalQuotaAllowlist == null) {
            System.clearProperty(QUOTA_ALLOWLIST);
        } else {
            System.setProperty(QUOTA_ALLOWLIST, originalQuotaAllowlist);
        }
    }

    /**
     * Checks that running a given Command returns the expected Map. Asserts
     * that all specified keys are present with values of the specified types
     * and that there are no extra entries.
     *
     * @param cmdName
     *            - the primary name of the command
     * @param kwargs
     *            - keyword arguments to the command
     * @param fields
     *            - the fields that are expected in the returned Map
     * @throws IOException
     * @throws InterruptedException
     */
    public void testCommand(String cmdName, Map<String, String> kwargs, Field... fields) throws IOException, InterruptedException {
        ZooKeeperServer zks = serverFactory.getZooKeeperServer();
        Map<String, Object> result = Commands.runCommand(cmdName, zks, kwargs).toMap();

        assertTrue(result.containsKey("command"));
        // This is only true because we're setting cmdName to the primary name
        assertEquals(cmdName, result.remove("command"));
        assertTrue(result.containsKey("error"));
        assertNull("error: " + result.get("error"), result.remove("error"));

        for (Field field : fields) {
            String k = field.key;
            assertTrue("Result from command "
                               + cmdName
                               + " missing field \""
                               + k
                               + "\""
                               + "\n"
                               + result, result.containsKey(k));
            Class<?> t = field.type;
            Object v = result.remove(k);
            assertTrue("\""
                               + k
                               + "\" field from command "
                               + cmdName
                               + " should be of type "
                               + t
                               + ", is actually of type "
                               + v.getClass(), t.isAssignableFrom(v.getClass()));
        }

        assertTrue("Result from command " + cmdName + " contains extra fields: " + result, result.isEmpty());
    }

    public void testCommand(String cmdName, Field... fields) throws IOException, InterruptedException {
        testCommand(cmdName, new HashMap<String, String>(), fields);
    }

    private static class Field {

        String key;
        Class<?> type;
        Field(String key, Class<?> type) {
            this.key = key;
            this.type = type;
        }

    }

    @Test
    public void testConfiguration() throws IOException, InterruptedException {
        testCommand("configuration", new Field("client_port", Integer.class), new Field("data_dir", String.class), new Field("data_log_dir", String.class), new Field("tick_time", Integer.class), new Field("max_client_cnxns", Integer.class), new Field("min_session_timeout", Integer.class), new Field("max_session_timeout", Integer.class), new Field("server_id", Long.class), new Field("client_port_listen_backlog", Integer.class));
    }

    @Test
    public void testConnections() throws IOException, InterruptedException {
        testCommand("connections", new Field("connections", Iterable.class), new Field("secure_connections", Iterable.class));
    }

    @Test
    public void testObservers() throws IOException, InterruptedException {
        testCommand("observers", new Field("synced_observers", Integer.class), new Field("observers", Iterable.class));
    }

    @Test
    public void testObserverConnectionStatReset() throws IOException, InterruptedException {
        testCommand("observer_connection_stat_reset");
    }

    @Test
    public void testConnectionStatReset() throws IOException, InterruptedException {
        testCommand("connection_stat_reset");
    }

    @Test
    public void testDump() throws IOException, InterruptedException {
        testCommand("dump", new Field("expiry_time_to_session_ids", Map.class), new Field("session_id_to_ephemeral_paths", Map.class));
    }

    @Test
    public void testEnvironment() throws IOException, InterruptedException {
        testCommand("environment", new Field("zookeeper.version", String.class), new Field("host.name", String.class), new Field("java.version", String.class), new Field("java.vendor", String.class), new Field("java.home", String.class), new Field("java.class.path", String.class), new Field("java.library.path", String.class), new Field("java.io.tmpdir", String.class), new Field("java.compiler", String.class), new Field("os.name", String.class), new Field("os.arch", String.class), new Field("os.version", String.class), new Field("user.name", String.class), new Field("user.home", String.class), new Field("user.dir", String.class), new Field("os.memory.free", String.class), new Field("os.memory.max", String.class), new Field("os.memory.total", String.class));
    }

    @Test
    public void testGetTraceMask() throws IOException, InterruptedException {
        testCommand("get_trace_mask", new Field("tracemask", Long.class));
    }

    @Test
    public void testIsReadOnly() throws IOException, InterruptedException {
        testCommand("is_read_only", new Field("read_only", Boolean.class));
    }

    @Test
    public void testLastSnapshot() throws IOException, InterruptedException {
        testCommand("last_snapshot", new Field("zxid", String.class), new Field("timestamp", Long.class));
    }

    @Test
    public void testMonitor() throws IOException, InterruptedException {
        ArrayList<Field> fields = new ArrayList<>(Arrays.asList(
                new Field("version", String.class),
                new Field("avg_latency", Double.class),
                new Field("max_latency", Long.class),
                new Field("min_latency", Long.class),
                new Field("packets_received", Long.class),
                new Field("packets_sent", Long.class),
                new Field("num_alive_connections", Integer.class),
                new Field("outstanding_requests", Long.class),
                new Field("server_state", String.class),
                new Field("znode_count", Integer.class),
                new Field("watch_count", Integer.class),
                new Field("ephemerals_count", Integer.class),
                new Field("approximate_data_size", Long.class),
                new Field("open_file_descriptor_count", Long.class),
                new Field("max_file_descriptor_count", Long.class),
                new Field("last_client_response_size", Integer.class),
                new Field("max_client_response_size", Integer.class),
                new Field("min_client_response_size", Integer.class),
                new Field("auth_failed_count", Long.class),
                new Field("non_mtls_remote_conn_count", Long.class),
                new Field("non_mtls_local_conn_count", Long.class),
                new Field("uptime", Long.class),
                new Field("global_sessions", Long.class),
                new Field("local_sessions", Long.class),
                new Field("connection_drop_probability", Double.class),
                new Field("outstanding_tls_handshake", Integer.class)
        ));
        Map<String, Object> metrics = MetricsUtils.currentServerMetrics();

        for (String metric : metrics.keySet()) {
            boolean alreadyDefined = fields.stream().anyMatch(f -> {
                return f.key.equals(metric);
            });
            if (alreadyDefined) {
                // known metrics are defined statically in the block above
                continue;
            }
            if (metric.startsWith("avg_")) {
                fields.add(new Field(metric, Double.class));
            } else {
                fields.add(new Field(metric, Long.class));
            }
        }
        Field[] fieldsArray = fields.toArray(new Field[0]);
        testCommand("monitor", fieldsArray);
    }

    @Test
    public void testRuok() throws IOException, InterruptedException {
        testCommand("ruok");
    }

    @Test
    public void testServerStats() throws IOException, InterruptedException {
        testCommand("server_stats", new Field("version", String.class), new Field("read_only", Boolean.class), new Field("server_stats", ServerStats.class), new Field("node_count", Integer.class), new Field("client_response", BufferStats.class));
    }

    @Test
    public void testSetTraceMask() throws IOException, InterruptedException {
        Map<String, String> kwargs = new HashMap<String, String>();
        kwargs.put("traceMask", "1");
        testCommand("set_trace_mask", kwargs, new Field("tracemask", Long.class));
    }

    @Test
    public void testStat() throws IOException, InterruptedException {
        testCommand("stats",
                    new Field("version", String.class),
                    new Field("read_only", Boolean.class),
                    new Field("server_stats", ServerStats.class),
                    new Field("node_count", Integer.class),
                    new Field("connections", Iterable.class),
                    new Field("secure_connections", Iterable.class),
                    new Field("client_response", BufferStats.class));
    }

    @Test
    public void testStatReset() throws IOException, InterruptedException {
        testCommand("stat_reset");
    }

    @Test
    public void testWatches() throws IOException, InterruptedException {
        testCommand("watches", new Field("session_id_to_watched_paths", Map.class));
    }

    @Test
    public void testWatchesByPath() throws IOException, InterruptedException {
        testCommand("watches_by_path", new Field("path_to_session_ids", Map.class));
    }

    @Test
    public void testWatchSummary() throws IOException, InterruptedException {
        testCommand("watch_summary", new Field("num_connections", Integer.class), new Field("num_paths", Integer.class), new Field("num_total_watches", Integer.class));
    }

    @Test
    public void testVotingViewCommand() throws IOException, InterruptedException {
        testCommand("voting_view",
                    new Field("current_config", Map.class));
    }

    @Test
    public void testQuotaStatsExactMetadata() throws Exception {
        createQuotaFixture();
        System.setProperty(QUOTA_ALLOWLIST, "[\"/quota-test\"]");

        CommandResponse response = quotaStats("/quota-test");
        assertQuotaResponse(response, "/quota-test", 2, 7L, 10, 100L, null);
        StringWriter output = new StringWriter();
        new JsonOutputter().output(response, new PrintWriter(output));
        JsonNode json = new ObjectMapper().readTree(output.toString());
        assertEquals(1, json.get("schema_version").intValue());
        assertEquals(2, json.get("count_used").intValue());
        assertEquals(7L, json.get("bytes_used").longValue());
        assertTrue(json.get("available").booleanValue());
        assertTrue(json.get("reason").isNull());
        assertTrue(json.get("error").isNull());
    }

    @Test
    public void testQuotaStatsUnlimitedAndZeroLimits() throws Exception {
        ZooKeeper zk = createQuotaFixture();
        System.setProperty(QUOTA_ALLOWLIST, "[\"/quota-test\"]");
        String[] limits = {
            "count=-1,bytes=100", "count=10,bytes=-1", "count=-1,bytes=-1", "count=0,bytes=0"
        };
        Integer[] counts = {null, 10, null, 0};
        Long[] bytes = {100L, null, null, 0L};
        for (int i = 0; i < limits.length; i++) {
            zk.setData(Quotas.quotaPath("/quota-test"), limits[i].getBytes(StandardCharsets.UTF_8), -1);
            CommandResponse response = quotaStats("/quota-test");
            assertQuotaResponse(response, "/quota-test", 2, 7L, counts[i], bytes[i], null);
            StringWriter output = new StringWriter();
            new JsonOutputter().output(response, new PrintWriter(output));
            JsonNode json = new ObjectMapper().readTree(output.toString());
            assertEquals(counts[i] == null, json.get("count_limit").isNull());
            assertEquals(bytes[i] == null, json.get("bytes_limit").isNull());
        }
    }

    @Test
    public void testQuotaStatsMissingMetadata() throws Exception {
        ZooKeeper zk = createClient();
        zk.create("/quota-test", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.setProperty(QUOTA_ALLOWLIST, "[\"/quota-test\"]");
        assertQuotaResponse(quotaStats("/quota-test"), "/quota-test", null, null, null, null, "quota_missing");
    }

    @Test
    public void testQuotaStatsMissingNamespace() {
        System.setProperty(QUOTA_ALLOWLIST, "[\"/quota-test\"]");
        assertQuotaResponse(quotaStats("/quota-test"), "/quota-test", null, null, null, null, "namespace_missing");
    }

    @Test
    public void testQuotaStatsDoesNotUseAncestorQuota() throws Exception {
        createQuotaFixture();
        System.setProperty(QUOTA_ALLOWLIST, "[\"/quota-test/child\"]");
        assertQuotaResponse(quotaStats("/quota-test/child"), "/quota-test/child",
                            null, null, null, null, "quota_missing");
    }

    @Test
    public void testQuotaStatsDefaultAndEmptyAllowlist() {
        assertQuotaError(quotaStats("/quota-test"), "not allowlisted");
        System.setProperty(QUOTA_ALLOWLIST, "[]");
        assertQuotaError(quotaStats("/quota-test"), "not allowlisted");
    }

    @Test
    public void testQuotaStatsExactAllowlist() {
        System.setProperty(QUOTA_ALLOWLIST, "[\"/quota-test\"]");
        for (String path : Arrays.asList("/quota-test/child", "/quota-test-other", "/Quota-test")) {
            assertQuotaError(quotaStats(path), "not allowlisted");
        }
        assertQuotaResponse(quotaStats("/quota-test"), "/quota-test", null, null, null, null, "namespace_missing");

        System.setProperty(QUOTA_ALLOWLIST, "[\"/quota-test/*\"]");
        assertQuotaError(quotaStats("/quota-test/child"), "not allowlisted");
        assertQuotaResponse(quotaStats("/quota-test/*"), "/quota-test/*",
                            null, null, null, null, "namespace_missing");
    }

    @Test
    public void testQuotaStatsRejectsMalformedAllowlist() {
        String[] configurations = {
            "", "null", "{}", "\"/quota-test\"", "[1]", "[null]", "[true]", "[[]]",
            "[\"/quota-test\"", "[\"/quota-test\",]", "[\"/quota-test\"] []",
            "[\"/quota-test\", \"sensitive-token\"]", "[\"/quota-test\", \"/\"]",
            "[\"/quota-test\", \"/zookeeper/quota\"]", "[\"/quota-test\", \"/other//child\"]",
            "['/quota-test']", "/* comment */[\"/quota-test\"]"
        };
        for (String configuration : configurations) {
            System.setProperty(QUOTA_ALLOWLIST, configuration);
            CommandResponse response = quotaStats("/quota-test");
            assertQuotaError(response, QUOTA_ALLOWLIST);
            assertFalse(response.getError().contains("sensitive-token"));
        }
    }

    @Test
    public void testQuotaStatsRejectsMissingOrInvalidPaths() {
        System.setProperty(QUOTA_ALLOWLIST, "[\"/quota-test\"]");
        assertQuotaError(Commands.runCommand("quota_stats", serverFactory.getZooKeeperServer(), null), "path");
        assertQuotaError(Commands.runCommand("quota_stats", serverFactory.getZooKeeperServer(),
                                            Collections.emptyMap()), "path");
        String[] paths = {
            null, "", "quota-test", "/", "/zookeeper", "/zookeeper/config", "/zookeeper/quota",
            "/quota-test/", "/quota-test//child", "/quota-test/.", "/quota-test/../child",
            "/quota-test/\u0000", "/quota-test/\n", "/quota-test/\ud800", "/quota-test/\uffff"
        };
        for (String path : paths) {
            assertQuotaError(quotaStats(path), "path");
        }
    }

    @Test
    public void testQuotaStatsValidPathComponents() throws Exception {
        String[] paths = {"/zookeeper-client", "/quota-test/child", "/quota-test/..child", "/quota-test/\u00e9"};
        System.setProperty(QUOTA_ALLOWLIST, new ObjectMapper().writeValueAsString(paths));
        for (String path : paths) {
            assertQuotaResponse(quotaStats(path), path, null, null, null, null, "namespace_missing");
        }
        System.setProperty(QUOTA_ALLOWLIST, " [ \"/quota-test\", \"/quota-test\" ] ");
        assertQuotaResponse(quotaStats("/quota-test"), "/quota-test", null, null, null, null, "namespace_missing");
    }

    @Test
    public void testQuotaStatsReflectsAllowlistChanges() {
        System.setProperty(QUOTA_ALLOWLIST, "[\"/quota-test\"]");
        assertQuotaResponse(quotaStats("/quota-test"), "/quota-test", null, null, null, null, "namespace_missing");
        System.clearProperty(QUOTA_ALLOWLIST);
        assertQuotaError(quotaStats("/quota-test"), "not allowlisted");
        System.setProperty(QUOTA_ALLOWLIST, "invalid");
        assertQuotaError(quotaStats("/quota-test"), QUOTA_ALLOWLIST);
    }

    @Test
    public void testQuotaStatsUninitializedServer() {
        String expected = Commands.runCommand("ruok", null, null).getError();
        for (ZooKeeperServer server : Arrays.asList(null, new ZooKeeperServer())) {
            CommandResponse response = Commands.runCommand("quota_stats", server,
                                                          Collections.singletonMap("path", "/quota-test"));
            assertQuotaError(response, expected);
        }
    }

    @Test
    public void testQuotaStatsMalformedMetadataReturnsUnavailable() throws Exception {
        ZooKeeper zk = createQuotaFixture();
        System.setProperty(QUOTA_ALLOWLIST, "[\"/quota-test\"]");
        zk.setData(Quotas.statPath("/quota-test"), "sensitive-token".getBytes(StandardCharsets.UTF_8), -1);
        CommandResponse response;
        try {
            response = quotaStats("/quota-test");
        } catch (RuntimeException e) {
            throw new AssertionError("Quota telemetry must report unavailable, not throw " + e.getClass().getSimpleName());
        }
        assertQuotaResponse(response, "/quota-test", null, null, null, null, "invalid_quota_stats");
        StringWriter output = new StringWriter();
        new JsonOutputter().output(response, new PrintWriter(output));
        assertFalse(output.toString().contains("sensitive-token"));
        JsonNode json = new ObjectMapper().readTree(output.toString());
        assertTrue(json.get("count_used").isNull());
        assertTrue(json.get("bytes_used").isNull());
        assertTrue(json.get("count_limit").isNull());
        assertTrue(json.get("bytes_limit").isNull());
        assertFalse(json.get("available").booleanValue());
    }

    @Test
    public void testQuotaStatsIncompleteMetadataReturnsUnavailable() throws Exception {
        ZooKeeper zk = createQuotaFixture();
        System.setProperty(QUOTA_ALLOWLIST, "[\"/quota-test\"]");
        zk.delete(Quotas.statPath("/quota-test"), -1);
        assertQuotaResponse(quotaStats("/quota-test"), "/quota-test", null, null, null, null, "quota_incomplete");
    }

    @Test
    public void testQuotaStatsDeniedConfigurationAccess() {
        SecurityManager original = System.getSecurityManager();
        try {
            System.setSecurityManager(new SecurityManager() {
                @Override
                public void checkPermission(Permission permission) {
                }

                @Override
                public void checkPropertyAccess(String key) {
                    if (QUOTA_ALLOWLIST.equals(key)) {
                        throw new SecurityException("sensitive-token");
                    }
                }
            });
            CommandResponse response = quotaStats("/quota-test");
            assertQuotaError(response, QUOTA_ALLOWLIST);
            assertFalse(response.getError().contains("sensitive-token"));
        } finally {
            System.setSecurityManager(original);
        }
    }

    @Test
    public void testQuotaStatsValidationPrecedesTreeAccess() {
        ZooKeeperServer server = mock(ZooKeeperServer.class);
        when(server.isRunning()).thenReturn(true);
        when(server.getZKDatabase()).thenThrow(new AssertionError("Rejected requests must not access the data tree"));
        assertQuotaError(Commands.runCommand("quota_stats", server, null), "path");
        Map<String, String> request = Collections.singletonMap("path", "/quota-test");
        assertQuotaError(Commands.runCommand("quota_stats", server, request), "not allowlisted");
        System.setProperty(QUOTA_ALLOWLIST, "[\"/quota-test\", null]");
        assertQuotaError(Commands.runCommand("quota_stats", server, request), QUOTA_ALLOWLIST);
    }

    @Test
    public void testQuotaStatsAfterServerRestart() throws Exception {
        createQuotaFixture();
        System.setProperty(QUOTA_ALLOWLIST, "[\"/quota-test\"]");
        assertQuotaResponse(quotaStats("/quota-test"), "/quota-test", 2, 7L, 10, 100L, null);
        stopServer();
        startServer();
        stopServer();
        startServer();
        assertQuotaResponse(quotaStats("/quota-test"), "/quota-test", 2, 7L, 10, 100L, null);
        System.clearProperty(QUOTA_ALLOWLIST);
        assertQuotaError(quotaStats("/quota-test"), "not allowlisted");
    }

    private ZooKeeper createQuotaFixture() throws Exception {
        ZooKeeper zk = createClient();
        zk.create("/quota-test", "abc".getBytes(StandardCharsets.UTF_8), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        SetQuotaCommand.createQuota(zk, "/quota-test", 100L, 10);
        zk.create("/quota-test/child", new byte[4], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        return zk;
    }

    private CommandResponse quotaStats(String path) {
        return Commands.runCommand("quota_stats", serverFactory.getZooKeeperServer(),
                                   Collections.singletonMap("path", path));
    }

    private void assertQuotaError(CommandResponse response, String message) {
        assertEquals("quota_stats", response.getCommand());
        assertNotNull(response.getError());
        assertTrue(response.getError(), response.getError().contains(message));
        Map<String, Object> result = response.toMap();
        assertEquals(response.getError(), result.get("error"));
        assertEquals(new HashSet<>(Arrays.asList("command", "error")), result.keySet());
    }

    private void assertQuotaResponse(CommandResponse response, String path, Integer countUsed, Long bytesUsed,
                                     Integer countLimit, Long bytesLimit, String reason) {
        assertEquals("quota_stats", response.getCommand());
        assertNull(response.getError(), response.getError());
        Map<String, Object> result = response.toMap();
        assertEquals(new HashSet<>(Arrays.asList("command", "error", "schema_version", "path",
                                                "count_used", "bytes_used", "count_limit", "bytes_limit",
                                                "available", "reason")), result.keySet());
        assertEquals(1, result.get("schema_version"));
        assertEquals(path, result.get("path"));
        assertEquals(countUsed, result.get("count_used"));
        assertEquals(bytesUsed, result.get("bytes_used"));
        assertEquals(countLimit, result.get("count_limit"));
        assertEquals(bytesLimit, result.get("bytes_limit"));
        assertEquals(reason == null, result.get("available"));
        assertEquals(reason, result.get("reason"));
    }

    @Test
    public void testConsCommandSecureOnly() {
        // Arrange
        Commands.ConsCommand cmd = new Commands.ConsCommand();
        ZooKeeperServer zkServer = mock(ZooKeeperServer.class);
        ServerCnxnFactory cnxnFactory = mock(ServerCnxnFactory.class);
        when(zkServer.getSecureServerCnxnFactory()).thenReturn(cnxnFactory);

        // Act
        CommandResponse response = cmd.run(zkServer, null);

        // Assert
        assertThat(response.toMap().containsKey("connections"), is(true));
        assertThat(response.toMap().containsKey("secure_connections"), is(true));
    }

    /**
     * testing Stat command, when only SecureClientPort is defined by the user and there is no
     * regular (non-SSL port) open. In this case zkServer.getServerCnxnFactory === null
     * see: ZOOKEEPER-3633
     */
    @Test
    public void testStatCommandSecureOnly() {
        Commands.StatCommand cmd = new Commands.StatCommand();
        ZooKeeperServer zkServer = mock(ZooKeeperServer.class);
        ServerCnxnFactory cnxnFactory = mock(ServerCnxnFactory.class);
        ServerStats serverStats = mock(ServerStats.class);
        ZKDatabase zkDatabase = mock(ZKDatabase.class);
        when(zkServer.getSecureServerCnxnFactory()).thenReturn(cnxnFactory);
        when(zkServer.serverStats()).thenReturn(serverStats);
        when(zkServer.getZKDatabase()).thenReturn(zkDatabase);
        when(zkDatabase.getNodeCount()).thenReturn(0);

        CommandResponse response = cmd.run(zkServer, null);

        assertThat(response.toMap().containsKey("connections"), is(true));
        assertThat(response.toMap().containsKey("secure_connections"), is(true));
    }

}
