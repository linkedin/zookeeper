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

package org.apache.zookeeper.server;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.OutputArchive;
import org.apache.zookeeper.Quotas;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.GetChildren2Response;
import org.apache.zookeeper.proto.GetChildrenResponse;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.server.SnapshotComparerTest.SnapshotToolTestSupport;
import org.apache.zookeeper.server.persistence.FileHeader;
import org.apache.zookeeper.server.persistence.FileSnap;
import org.apache.zookeeper.server.persistence.SnapStream.StreamMode;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class OfflineAuditExporterTest extends ZKTestCase {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String MAIN = "org.apache.zookeeper.server.OfflineAuditExporter";
    private SnapshotToolTestSupport support;
    private int nextOutput;

    @Before
    public void setUp() throws IOException {
        support = new SnapshotToolTestSupport();
    }

    @After
    public void tearDown() throws IOException {
        support.close();
    }

    @Test
    public void childResponseSizesIncludeProtocolOverhead() throws Exception {
        List<String> names = Arrays.asList("\u00e9", "a");
        assertEquals(31L, OfflineAuditExporter.childResponseBytes(names, false));
        assertEquals(99L, OfflineAuditExporter.childResponseBytes(names, true));
        assertEquals(20L, OfflineAuditExporter.childResponseBytes(Collections.<String>emptyList(), false));
        assertEquals(88L, OfflineAuditExporter.childResponseBytes(Collections.<String>emptyList(), true));
        assertEquals(28L, OfflineAuditExporter.childResponseBytes(Collections.singletonList("\ud83d\ude00"), false));
    }

    @Test
    public void childResponseSizesMatchActualJuteSerialization() throws Exception {
        for (List<String> names : Arrays.asList(Collections.<String>emptyList(),
                                                Arrays.asList("a", "\u00e9", "\u4e2d", "quote\""))) {
            for (boolean includeStat : new boolean[]{false, true}) {
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                OutputArchive archive = BinaryOutputArchive.getArchive(bytes);
                new ReplyHeader(123, 456L, 0).serialize(archive, "header");
                if (includeStat) {
                    new GetChildren2Response(names, new Stat()).serialize(archive, "response");
                } else {
                    new GetChildrenResponse(names).serialize(archive, "response");
                }
                assertEquals(bytes.size(), OfflineAuditExporter.childResponseBytes(names, includeStat));
            }
        }
    }

    @Test
    public void childResponseSizesUseLongArithmetic() throws Exception {
        char[] name = new char[65532];
        Arrays.fill(name, 'a');
        assertEquals(2147483668L, OfflineAuditExporter.childResponseBytes(Collections.nCopies(32768, new String(name)), false));
    }

    @Test
    public void exportsRootUnicodeAndPersistedMetadataWithoutSecrets() throws Exception {
        DataTree tree = customerTree();
        tree.setData("/app/\u00e9", "private-synthetic-data".getBytes(StandardCharsets.UTF_8), 2, 3, 1234);
        DataNode unicode = tree.getNode("/app/\u00e9");
        unicode.stat.setCzxid(0x2000000000000001L);
        unicode.stat.setMzxid(0x7ffffffffffffff0L);
        unicode.stat.setPzxid(0x8000000000000001L);
        unicode.stat.setCtime(1000);
        tree.setACL("/app/\u00e9", Collections.singletonList(
            new ACL(ZooDefs.Perms.READ, new Id("digest", "synthetic-user:do-not-export"))), 4);
        File snapshot = support.snapshot(tree, StreamMode.CHECKED, true);
        byte[] original = Files.readAllBytes(snapshot.toPath());
        FileTime mtime = Files.getLastModifiedTime(snapshot.toPath());
        Path output = output();

        run(snapshot, output).success();

        JsonNode root = node(output, "/");
        assertEquals(5L, root.get("data_length").asLong());
        assertEquals(4L, root.get("num_children").asLong());
        assertEquals(1L, root.get("path_utf8_bytes").asLong());
        JsonNode record = node(output, "/app/\u00e9");
        assertEquals(1, record.get("schema_version").asInt());
        assertEquals(22L, record.get("data_length").asLong());
        assertEquals(7L, record.get("path_utf8_bytes").asLong());
        assertEquals(0L, record.get("num_children").asLong());
        assertEquals(20L, record.get("getchildren_response_bytes").asLong());
        assertEquals(88L, record.get("getchildren2_response_bytes").asLong());
        assertEquals(1000L, record.get("ctime_ms").asLong());
        assertEquals(1234L, record.get("mtime_ms").asLong());
        assertEquals("0x2000000000000001", record.get("czxid").asText());
        assertEquals("0x7ffffffffffffff0", record.get("mzxid").asText());
        assertEquals("0x8000000000000001", record.get("pzxid").asText());
        assertEquals(2, record.get("data_version").asInt());
        assertEquals(4, record.get("acl_version").asInt());
        assertEquals(0, record.get("persisted_cversion").asInt());
        assertEquals("persistent", record.get("node_type").asText());
        assertEquals("0x0000000000000000", record.get("ephemeral_owner").asText());
        assertTrue(record.get("ttl_ms").isNull());
        assertEquals(9, records(output.resolve("nodes.ndjson")).size());
        String all = text(output.resolve("nodes.ndjson"));
        assertThat(all, not(containsString("private-synthetic-data")));
        assertThat(all, not(containsString("synthetic-user")));
        assertThat(all, not(containsString("do-not-export")));
        assertThat(all, not(containsString("\"path\":\"\"")));
        assertEquals("0x8000000000000001", manifest(output).get("last_observed_zxid").asText());
        assertArrayEquals(original, Files.readAllBytes(snapshot.toPath()));
        assertEquals(mtime, Files.getLastModifiedTime(snapshot.toPath()));
    }

    @Test
    public void nodeOrderIsDeterministicPreorderWithContiguousSubtrees() throws Exception {
        DataTree tree = new DataTree();
        for (String path : Arrays.asList("/a-", "/a-/y", "/a", "/a/x", "/a/x/z")) {
            support.addNode(tree, path, null);
        }
        File snapshot = support.snapshot(tree, StreamMode.CHECKED, false);
        Path first = output();
        Path second = output();
        run(snapshot, first).success();
        run(snapshot, second).success();
        List<String> paths = new ArrayList<>();
        for (JsonNode node : records(first.resolve("nodes.ndjson"))) {
            paths.add(node.get("path").asText());
        }
        assertEquals(Arrays.asList("/", "/a", "/a/x", "/a/x/z", "/a-", "/a-/y",
                                   "/zookeeper", "/zookeeper/config", "/zookeeper/quota"), paths);
        assertArrayEquals(Files.readAllBytes(first.resolve("nodes.ndjson")), Files.readAllBytes(second.resolve("nodes.ndjson")));
    }

    @Test
    public void namespaceTotalsAreInclusiveAndExcludeOnlyReservedSubtree() throws Exception {
        DataTree tree = customerTree();
        addQuota(tree, "/app", "count=50,bytes=1000");
        Path output = output();

        run(support.snapshot(tree, StreamMode.CHECKED, false), output).success();

        assertEquals(3, records(output.resolve("namespaces.ndjson")).size());
        JsonNode app = record(output.resolve("namespaces.ndjson"), "path", "/app");
        assertEquals(3L, app.get("subtree_node_count").asLong());
        assertEquals(12L, app.get("payload_bytes").asLong());
        assertEquals(17L, app.get("path_utf8_bytes").asLong());
        assertEquals("valid", app.get("quota_status").asText());
        assertEquals(50, app.get("quota_limit_count").asInt());
        assertEquals(1000L, app.get("quota_limit_bytes").asLong());
        assertTrue(app.get("quota_stat_present").asBoolean());
        JsonNode client = record(output.resolve("namespaces.ndjson"), "path", "/zookeeper-client");
        assertEquals(1L, client.get("subtree_node_count").asLong());
        assertEquals(11L, client.get("payload_bytes").asLong());
        assertEquals(17L, client.get("path_utf8_bytes").asLong());
        assertEquals("absent", client.get("quota_status").asText());
        assertTrue(client.get("quota_limit_bytes").isNull());
        assertFalse(client.get("quota_stat_present").asBoolean());
        assertThat(text(output.resolve("namespaces.ndjson")), not(containsString("\"path\":\"/zookeeper\"")));
        assertNotNull(node(output, "/zookeeper/quota/app/zookeeper_limits"));
    }

    @Test
    public void quotaAvailabilityDoesNotInventOrLeakMalformedLimits() throws Exception {
        DataTree tree = customerTree();
        addQuota(tree, "/app", "private-malformed-quota");
        addQuota(tree, "/other", "count=-1,bytes=999");
        Path output = output();

        run(support.snapshot(tree, StreamMode.CHECKED, false), output).success();

        JsonNode invalid = record(output.resolve("namespaces.ndjson"), "path", "/app");
        assertEquals("invalid", invalid.get("quota_status").asText());
        assertTrue(invalid.get("quota_limit_count").isNull());
        assertTrue(invalid.get("quota_limit_bytes").isNull());
        JsonNode valid = record(output.resolve("namespaces.ndjson"), "path", "/other");
        assertEquals("valid", valid.get("quota_status").asText());
        assertEquals(-1, valid.get("quota_limit_count").asInt());
        assertEquals(999, valid.get("quota_limit_bytes").asLong());
        assertThat(text(output.resolve("namespaces.ndjson")), not(containsString("private-malformed-quota")));
    }

    @Test
    public void aclFlagsAreReviewHintsWithoutRawIdentities() throws Exception {
        DataTree tree = new DataTree();
        List<List<ACL>> acls = Arrays.asList(
            ZooDefs.Ids.OPEN_ACL_UNSAFE,
            Collections.singletonList(new ACL(ZooDefs.Perms.READ, new Id("world", "anyone"))),
            Collections.singletonList(new ACL(ZooDefs.Perms.READ, new Id("auth", ""))),
            Collections.singletonList(new ACL(ZooDefs.Perms.ALL, new Id("custom-secret-scheme", "private-id"))),
            Collections.<ACL>emptyList(),
            Collections.singletonList(new ACL(64, new Id("digest", "private-digest"))),
            Collections.singletonList(new ACL(ZooDefs.Perms.READ, new Id("world", "private-invalid-identity"))));
        List<Set<String>> expected = Arrays.asList(
            flags("world_read", "world_write", "world_admin"), flags("world_read"),
            flags("auth"), flags("unknown_scheme"), flags("missing_acl"), flags("invalid_permissions"), flags("invalid_identity"));
        for (int i = 0; i < acls.size(); i++) {
            tree.createNode("/acl" + i, null, acls.get(i), 0, -1, 1, 1);
        }
        Path output = output();

        run(support.snapshot(tree, StreamMode.CHECKED, false), output).success();

        for (int i = 0; i < expected.size(); i++) {
            Set<String> actual = new HashSet<>();
            for (JsonNode flag : node(output, "/acl" + i).get("acl_risk_flags")) {
                actual.add(flag.asText());
            }
            assertEquals(expected.get(i), actual);
            assertEquals(0, node(output, "/acl" + i).get("data_length").asLong());
        }
        assertThat(text(output.resolve("nodes.ndjson")), not(containsString("private-")));
        assertThat(text(output.resolve("nodes.ndjson")), not(containsString("custom-secret-scheme")));
    }

    @Test
    public void unknownConfigurationDoesNotTurnTtlOrUnknownOwnersIntoSessions() throws Exception {
        Path output = output();
        run(support.snapshot(ownerTree(true), StreamMode.CHECKED, true), output).success();

        assertType(output, "/session", "ephemeral", "session");
        assertType(output, "/container", "container", "container");
        for (String path : Arrays.asList("/ttl", "/legacy", "/unknown")) {
            assertType(output, path, "unknown", "unknown");
        }
        List<JsonNode> sessions = records(output.resolve("sessions.ndjson"));
        assertEquals(1, sessions.size());
        JsonNode session = sessions.get(0);
        assertEquals("0x0000000000000123", session.get("session_id").asText());
        assertEquals(2, session.get("ephemeral_node_count").asLong());
        assertEquals(12, session.get("payload_bytes").asLong());
        assertEquals(15, session.get("path_utf8_bytes").asLong());
        assertTrue(session.get("present_in_snapshot_session_table").asBoolean());
        assertEquals(30000, session.get("timeout_ms").asInt());
        assertEquals(3, manifest(output).get("unknown_owner_nodes").asLong());
        assertTrue(manifest(output).get("decoder").get("source_extended_types_enabled").isNull());
    }

    @Test
    public void explicitModernConfigurationDecodesTtlAndContainer() throws Exception {
        Path output = output();
        run(support.snapshot(ownerTree(false), StreamMode.CHECKED, true), output,
            Arrays.asList("-Dzookeeper.extendedTypesEnabled=true", "-Dzookeeper.emulate353TTLNodes=false")).success();

        assertType(output, "/ttl", "ttl", "ttl");
        assertEquals(42, node(output, "/ttl").get("ttl_ms").asLong());
        assertType(output, "/legacy", "ephemeral", "session");
        assertType(output, "/container", "container", "container");
        assertEquals(2, records(output.resolve("sessions.ndjson")).size());
        JsonNode orphan = record(output.resolve("sessions.ndjson"), "session_id", "0x800000000000002a");
        assertFalse(orphan.get("present_in_snapshot_session_table").asBoolean());
        assertTrue(orphan.get("timeout_ms").isNull());
        assertTrue(manifest(output).get("decoder").get("source_extended_types_enabled").asBoolean());
        assertFalse(manifest(output).get("decoder").get("source_emulate_353_ttl_nodes").asBoolean());
    }

    @Test
    public void explicitLegacyConfigurationUsesNativeTtlValueSemantics() throws Exception {
        DataTree tree = ownerTree(false);
        tree.getNode("/legacy").stat.setEphemeralOwner(0x800001000000002aL);
        Path output = output();
        run(support.snapshot(tree, StreamMode.CHECKED, false), output,
            Arrays.asList("-Dzookeeper.extendedTypesEnabled=true", "-Dzookeeper.emulate353TTLNodes=true")).success();

        assertType(output, "/legacy", "ttl", "ttl-3.5.3");
        assertEquals(42, node(output, "/legacy").get("ttl_ms").asLong());
        assertEquals(1, records(output.resolve("sessions.ndjson")).size());
    }

    @Test
    public void explicitDisabledExtendedTypesPreservesNegativeSessionIds() throws Exception {
        Path output = output();
        run(support.snapshot(ownerTree(true), StreamMode.CHECKED, false), output,
            Collections.singletonList("-Dzookeeper.extendedTypesEnabled=false")).success();

        assertType(output, "/ttl", "ephemeral", "session");
        assertType(output, "/unknown", "ephemeral", "session");
        assertType(output, "/container", "container", "container");
        assertEquals(4, records(output.resolve("sessions.ndjson")).size());
    }

    @Test
    public void missingLegacySettingRemainsUnknownForNegativeEncodings() throws Exception {
        Path output = output();
        run(support.snapshot(ownerTree(false), StreamMode.CHECKED, false), output,
            Collections.singletonList("-Dzookeeper.extendedTypesEnabled=true")).success();

        assertType(output, "/legacy", "unknown", "unknown");
        assertType(output, "/ttl", "unknown", "unknown");
        assertEquals(1, records(output.resolve("sessions.ndjson")).size());
    }

    @Test
    public void unsupportedKnownExtendedEncodingFailsClosed() throws Exception {
        Path output = output();
        failure(run(support.snapshot(ownerTree(true), StreamMode.CHECKED, false), output,
                    Arrays.asList("-Dzookeeper.extendedTypesEnabled=true", "-Dzookeeper.emulate353TTLNodes=false")));
        assertFalse(Files.exists(output.resolve("manifest.json")));
    }

    @Test
    public void invalidDecoderPropertiesAreNotSilentlyDefaulted() throws Exception {
        File snapshot = support.snapshot(new DataTree(), StreamMode.CHECKED, false);
        for (List<String> properties : Arrays.asList(
            Collections.singletonList("-Dzookeeper.extendedTypesEnabled=maybe"),
            Collections.singletonList("-Dzookeeper.emulate353TTLNodes=yes"),
            Collections.singletonList("-Dzookeeper.emulate353TTLNodes=true"),
            Collections.singletonList("-Djute.maxbuffer=not-a-number"),
            Collections.singletonList("-Djute.maxbuffer=-1"),
            Arrays.asList("-Djute.maxbuffer=2147483647", "-Dzookeeper.jute.maxbuffer.extrasize=1024"),
            Collections.singletonList("-Dzookeeper.jute.maxbuffer.extrasize=-1"))) {
            Path output = output();
            run(snapshot, output, properties).invalidInvocation();
            assertFalse(Files.exists(output));
        }
    }

    @Test
    public void nativeJutePropertySyntaxAndMinimumExtraPaddingAreRecorded() throws Exception {
        DataTree tree = new DataTree();
        support.addNode(tree, "/large", new byte[8000]);
        File snapshot = support.snapshot(tree, StreamMode.CHECKED, false);
        Path output = output();
        run(snapshot, output, Arrays.asList("-Djute.maxbuffer=0x1000", "-Dzookeeper.jute.maxbuffer.extrasize=010000")).success();
        assertEquals(8000, node(output, "/large").get("data_length").asLong());
        assertEquals(4096, manifest(output).get("decoder").get("jute_maxbuffer").asInt());
        assertEquals(4096, manifest(output).get("decoder").get("jute_extra_maxbuffer").asInt());
        File small = support.snapshot(new DataTree(), StreamMode.CHECKED, false);
        Path padded = output();
        run(small, padded, Arrays.asList("-Djute.maxbuffer=1024", "-Dzookeeper.jute.maxbuffer.extrasize=0")).success();
        assertEquals(1024, manifest(padded).get("decoder").get("jute_extra_maxbuffer").asInt());
    }

    @Test
    public void manifestRecordsChecksumsAndUnknownCaptureRatherThanRecoveryClaims() throws Exception {
        for (StreamMode mode : StreamMode.values()) {
            for (boolean digest : new boolean[]{false, true}) {
                File snapshot = support.snapshot(customerTree(), mode, digest);
                Path output = output();
                run(snapshot, output).success();
                JsonNode manifest = manifest(output);
                assertEquals(1, manifest.get("schema_version").asInt());
                assertTrue(manifest.get("export_complete").asBoolean());
                assertEquals("snapshot-only", manifest.get("recovery_scope").asText());
                assertFalse(manifest.get("transaction_logs_replayed").asBoolean());
                assertTrue(manifest.get("source_capture_time_ms").isNull());
                assertTrue(manifest.get("source_capture_provenance").isNull());
                assertTrue(manifest.get("source_server_version").isNull());
                assertTrue(manifest.get("max_output_bytes").isNull());
                assertEquals(snapshot.getCanonicalPath(), manifest.get("source").get("path").asText());
                assertEquals(Files.size(snapshot.toPath()), manifest.get("source").get("size_bytes").asLong());
                assertEquals(snapshot.lastModified(), manifest.get("source").get("mtime_ms").asLong());
                assertEquals(sha256(snapshot.toPath()), manifest.get("source").get("sha256").asText());
                assertEquals(2, manifest.get("source").get("format_version").asInt());
                assertEquals("OfflineAuditExporter", manifest.get("tool").get("name").asText());
                assertFalse(manifest.get("tool").get("version").asText().isEmpty());
                assertTrue(manifest.get("snapshot_zxid").asText().startsWith("0x"));
                assertEquals(!digest, manifest.get("snapshot_digest").isNull());
                if (digest) {
                    assertTrue(manifest.get("snapshot_digest").get("seal_validated").asBoolean());
                    assertFalse(manifest.get("snapshot_digest").get("transaction_consistency_verified").asBoolean());
                }
                Set<String> names = new HashSet<>();
                for (JsonNode descriptor : manifest.get("files")) {
                    Path file = output.resolve(descriptor.get("name").asText());
                    names.add(file.getFileName().toString());
                    assertEquals(Files.size(file), descriptor.get("size_bytes").asLong());
                    assertEquals(sha256(file), descriptor.get("sha256").asText());
                    assertEquals(records(file).size(), descriptor.get("records").asLong());
                }
                assertEquals(flags("nodes.ndjson", "namespaces.ndjson", "sessions.ndjson"), names);
            }
        }
    }

    @Test
    public void fuzzyDigestDoesNotRequireTransactionLogsOrBecomeAnEndpoint() throws Exception {
        DataTree tree = new DataTree() {
            @Override
            public boolean serializeZxidDigest(OutputArchive archive) throws IOException {
                new ZxidDigest(0x123456789abcdef0L, 2, 0xfedcba9876543210L).serialize(archive);
                return true;
            }
        };
        support.addNode(tree, "/customer", new byte[3]);
        File snapshot = support.snapshot(tree, StreamMode.CHECKED, true);
        Path output = output();

        run(snapshot, output).success();

        JsonNode manifest = manifest(output);
        assertEquals("0x0000000000000001", manifest.get("last_observed_zxid").asText());
        assertEquals("0x123456789abcdef0", manifest.get("snapshot_digest").get("zxid").asText());
        assertEquals("0xfedcba9876543210", manifest.get("snapshot_digest").get("value").asText());
        Path renamed = snapshot.toPath().resolveSibling("snapshot.11-22");
        Files.copy(snapshot.toPath(), renamed);
        Path renamedOutput = output();
        run(renamed.toFile(), renamedOutput).success();
        assertTrue(manifest(renamedOutput).get("snapshot_zxid").isNull());
    }

    @Test
    public void rejectsCorruptionWithoutFallbackToAnotherSnapshot() throws Exception {
        support.snapshot(customerTree(), StreamMode.CHECKED, true);
        for (File snapshot : support.corruptSnapshots()) {
            Path output = output();
            failure(run(snapshot, output));
            assertFalse(Files.exists(output.resolve("manifest.json")));
        }
    }

    @Test
    public void rejectsSealedUnsupportedFormatAndMissingRoot() throws Exception {
        File unsupported = new File(support.directory, "snapshot.abc");
        new FileSnap(null) {
            @Override
            protected void serialize(DataTree tree, Map<Long, Integer> sessions, OutputArchive archive,
                                     FileHeader header) throws IOException {
                super.serialize(tree, sessions, archive, new FileHeader(FileSnap.SNAP_MAGIC, 3, -1));
            }
        }.serialize(new DataTree(), Collections.<Long, Integer>emptyMap(), unsupported, false);
        Path unsupportedOutput = output();
        failure(run(unsupported, unsupportedOutput));
        assertFalse(Files.exists(unsupportedOutput.resolve("manifest.json")));
        DataTree missingRoot = new DataTree() {
            @Override
            public void serializeNodes(OutputArchive archive) throws IOException {
                archive.writeString("/", "path");
            }
        };
        Path rootOutput = output();
        failure(run(support.snapshot(missingRoot, StreamMode.CHECKED, false), rootOutput));
        assertFalse(Files.exists(rootOutput.resolve("manifest.json")));
    }

    @Test
    public void rejectsSealedDuplicateNodesInsteadOfDoubleCountingOwners() throws Exception {
        DataTree tree = new DataTree() {
            @Override
            public void serializeNodes(OutputArchive archive) throws IOException {
                for (String path : Arrays.asList("", "/zookeeper", "/zookeeper/config", "/zookeeper/quota", "/same")) {
                    serializeNodeData(archive, path, getNode(path));
                }
                DataNode duplicate = new DataNode(new byte[7], getNode("/same").acl, createStat(2, 2, 0x456L));
                serializeNodeData(archive, "/same", duplicate);
                archive.writeString("/", "path");
            }
        };
        tree.createNode("/same", new byte[5], ZooDefs.Ids.OPEN_ACL_UNSAFE, 0x123L, -1, 1, 1);
        Path output = output();
        failure(run(support.snapshot(tree, StreamMode.CHECKED, true), output));
        assertFalse(Files.exists(output.resolve("manifest.json")));
    }

    @Test
    public void rejectsInvalidCliArgumentsBeforeCreatingOutput() throws Exception {
        File snapshot = support.snapshot(customerTree(), StreamMode.CHECKED, false);
        execute(Collections.<String>emptyList()).invalidInvocation();
        execute(Collections.<String>emptyList(), "--snapshot-file", snapshot.toString()).invalidInvocation();
        for (String limit : Arrays.asList("0", "-1", "NaN", "9223372036854775808")) {
            Path output = output();
            run(snapshot, output, Collections.<String>emptyList(), "--max-output-bytes", limit).invalidInvocation();
            assertFalse(Files.exists(output));
        }
        Path output = output();
        run(snapshot, output, Collections.<String>emptyList(), "unexpected").invalidInvocation();
        run(snapshot, output, Collections.<String>emptyList(), "--unknown").invalidInvocation();
        run(snapshot, output, Collections.<String>emptyList(),
            "--snapshot-file", snapshot.toString()).invalidInvocation();
        run(new File(support.directory, "missing"), output).invalidInvocation();
        run(support.directory, output).invalidInvocation();
        assertFalse(Files.exists(output));
    }

    @Test
    public void rejectsExistingOutputAndAliasesWithoutTouchingUnrelatedFiles() throws Exception {
        File snapshot = support.snapshot(customerTree(), StreamMode.CHECKED, true);
        byte[] original = Files.readAllBytes(snapshot.toPath());
        Path output = output();
        run(snapshot, output).success();
        byte[] priorManifest = Files.readAllBytes(output.resolve("manifest.json"));
        run(snapshot, output).invalidInvocation();
        assertArrayEquals(priorManifest, Files.readAllBytes(output.resolve("manifest.json")));
        run(snapshot, snapshot.toPath()).invalidInvocation();
        run(snapshot, snapshot.toPath().getParent()).invalidInvocation();
        Path existing = output();
        Files.createDirectory(existing);
        Path unrelated = existing.resolve("unrelated.txt");
        Files.write(unrelated, new byte[]{1, 2, 3});
        run(snapshot, existing).invalidInvocation();
        assertArrayEquals(new byte[]{1, 2, 3}, Files.readAllBytes(unrelated));
        Path link = support.directory.toPath().resolve("source-hardlink");
        Files.createLink(link, snapshot.toPath());
        run(snapshot, link).invalidInvocation();
        assertArrayEquals(original, Files.readAllBytes(snapshot.toPath()));
    }

    @Test
    public void rejectsSymbolicOutputAliases() throws Exception {
        Assume.assumeFalse(System.getProperty("os.name").startsWith("Windows"));
        File snapshot = support.snapshot(customerTree(), StreamMode.CHECKED, true);
        Path link = support.directory.toPath().resolve("output-link");
        Path dangling = support.directory.toPath().resolve("dangling-link");
        try {
            Files.createSymbolicLink(link, snapshot.toPath().getParent());
            run(snapshot, link).invalidInvocation();
            Files.createSymbolicLink(dangling, support.directory.toPath().resolve("missing-target"));
            run(snapshot, dangling).invalidInvocation();
            assertTrue(Files.isSymbolicLink(dangling));
        } finally {
            Files.deleteIfExists(link);
            Files.deleteIfExists(dangling);
        }
    }

    @Test
    public void symlinkParentComponentsDoNotSelectADifferentSnapshotOrDestination() throws Exception {
        Assume.assumeFalse(System.getProperty("os.name").startsWith("Windows"));
        Path real = support.directory.toPath().resolve("real");
        Files.createDirectories(real.resolve("inner"));
        Path alias = support.directory.toPath().resolve("alias");
        DataTree selectedTree = new DataTree();
        support.addNode(selectedTree, "/selected", new byte[7]);
        Path selected = real.resolve("snapshot.55");
        Files.copy(support.snapshot(selectedTree, StreamMode.CHECKED, false).toPath(), selected);
        DataTree decoy = new DataTree();
        support.addNode(decoy, "/selected", new byte[17]);
        Files.copy(support.snapshot(decoy, StreamMode.CHECKED, false).toPath(),
                   support.directory.toPath().resolve("snapshot.55"));
        try {
            Files.createSymbolicLink(alias, real.resolve("inner"));
            Path input = alias.resolve("../snapshot.55");
            Path output = alias.resolve("../export");
            run(input.toFile(), output).success();
            assertTrue(Files.exists(real.resolve("export/manifest.json")));
            assertEquals(selected.toRealPath().toString(), manifest(output).get("source").get("path").asText());
            assertEquals(7, node(output, "/selected").get("data_length").asLong());
            assertFalse(Files.exists(support.directory.toPath().resolve("export")));
        } finally {
            Files.deleteIfExists(alias);
        }
    }

    @Test
    public void outputBudgetFailuresNeverPublishSuccessEvenAfterAllDataFiles() throws Exception {
        File snapshot = support.snapshot(customerTree(), StreamMode.CHECKED, false);
        Path complete = output();
        run(snapshot, complete).success();
        long dataBytes = 0;
        for (JsonNode file : manifest(complete).get("files")) {
            dataBytes += file.get("size_bytes").asLong();
        }
        for (long limit : new long[]{1, 1500, dataBytes + 1}) {
            Path output = output();
            failure(run(snapshot, output, Collections.<String>emptyList(),
                        "--max-output-bytes", Long.toString(limit)));
            assertFalse(Files.exists(output.resolve("manifest.json")));
            assertTrue(Files.exists(output.resolve("nodes.ndjson")));
            assertTrue(Files.size(output.resolve("nodes.ndjson")) <= limit);
            if (limit == dataBytes + 1) {
                assertTrue(Files.exists(output.resolve("sessions.ndjson")));
                assertEquals(Files.size(complete.resolve("nodes.ndjson")), Files.size(output.resolve("nodes.ndjson")));
            }
            run(snapshot, output).invalidInvocation();
        }
        Path bounded = output();
        run(snapshot, bounded, Collections.<String>emptyList(), "--max-output-bytes", "1000000").success();
        assertEquals(1000000L, manifest(bounded).get("max_output_bytes").asLong());
    }

    @Test
    public void largePayloadHonorsNativeJuteDecoderLimits() throws Exception {
        DataTree tree = new DataTree();
        support.addNode(tree, "/large", new byte[3000000]);
        File snapshot = support.snapshot(tree, StreamMode.CHECKED, false);
        Path tooSmall = output();
        failure(run(snapshot, tooSmall));
        assertFalse(Files.exists(tooSmall.resolve("manifest.json")));
        Path output = output();
        run(snapshot, output, Collections.singletonList("-Djute.maxbuffer=4194304")).success();
        assertEquals(3000000L, node(output, "/large").get("data_length").asLong());
        assertEquals(4194304, manifest(output).get("decoder").get("jute_maxbuffer").asInt());
        assertEquals(4194304, manifest(output).get("decoder").get("jute_extra_maxbuffer").asInt());
        assertTrue(Files.size(output.resolve("nodes.ndjson")) < 10000);
    }

    @Test
    public void largeFanoutReportsImmediateChildrenAndCompleteNamespaceTotals() throws Exception {
        DataTree tree = new DataTree();
        support.addNode(tree, "/fanout", null);
        for (int i = 0; i < 4096; i++) {
            support.addNode(tree, String.format("/fanout/child-%04d", i), new byte[1]);
        }
        Path output = output();
        run(support.snapshot(tree, StreamMode.CHECKED, false), output).success();
        JsonNode node = node(output, "/fanout");
        assertEquals(4096, node.get("num_children").asLong());
        assertEquals(57364L, node.get("getchildren_response_bytes").asLong());
        assertEquals(57432L, node.get("getchildren2_response_bytes").asLong());
        JsonNode namespace = record(output.resolve("namespaces.ndjson"), "path", "/fanout");
        assertEquals(4097L, namespace.get("subtree_node_count").asLong());
        assertEquals(4096L, namespace.get("payload_bytes").asLong());
        assertEquals(73735L, namespace.get("path_utf8_bytes").asLong());
    }

    @Test
    public void outputFilesystemFailureDoesNotPublishAResult() throws Exception {
        Assume.assumeTrue(support.directory.toPath().getFileSystem().supportedFileAttributeViews().contains("posix"));
        Path parent = support.directory.toPath().resolve("unwritable");
        Files.createDirectory(parent);
        File snapshot = support.snapshot(customerTree(), StreamMode.CHECKED, false);
        try {
            Files.setPosixFilePermissions(parent, PosixFilePermissions.fromString("r-x------"));
            Assume.assumeFalse(Files.isWritable(parent));
            Path output = parent.resolve("export");
            failure(run(snapshot, output));
            assertFalse(Files.exists(output.resolve("manifest.json")));
        } finally {
            Files.setPosixFilePermissions(parent, PosixFilePermissions.fromString("rwx------"));
        }
    }

    @Test
    public void rejectsInvalidPersistedPathsRatherThanPublishingNormalizedNames() throws Exception {
        for (String path : Arrays.asList("/bad\nname", "/bad\u0000name", "/bad\ud83d\ude00")) {
            DataTree tree = new DataTree();
            support.addNode(tree, path, null);
            Path output = output();
            failure(run(support.snapshot(tree, StreamMode.CHECKED, false), output));
            assertFalse(Files.exists(output.resolve("manifest.json")));
        }
    }

    @Test
    public void traversalHandlesDeepTreesWithoutRecursiveExporterStack() throws Exception {
        DataTree tree = new DataTree() {
            @Override
            public void serializeNodes(OutputArchive archive) throws IOException {
                Deque<String> paths = new ArrayDeque<>();
                paths.push("");
                while (!paths.isEmpty()) {
                    String path = paths.pop();
                    DataNode node = getNode(path);
                    serializeNodeData(archive, path, node);
                    for (String child : node.getChildren()) {
                        paths.push(path + "/" + child);
                    }
                }
                archive.writeString("/", "path");
            }
        };
        String path = "";
        for (int i = 0; i < 1800; i++) {
            path += "/d";
            support.addNode(tree, path, new byte[1]);
        }
        Path output = output();

        run(support.snapshot(tree, StreamMode.CHECKED, false), output,
            Collections.singletonList("-Xss256k")).success();

        JsonNode namespace = record(output.resolve("namespaces.ndjson"), "path", "/d");
        assertEquals(1800, namespace.get("subtree_node_count").asLong());
        assertEquals(1800, namespace.get("payload_bytes").asLong());
        assertEquals(3241800L, namespace.get("path_utf8_bytes").asLong());
        assertEquals(1804L, manifest(output).get("files").get(0).get("records").asLong());
    }

    @Test
    public void detectsSourceByteChangesEvenWhenSizeAndMtimeAreRestored() throws Exception {
        sourceMutation(0);
    }

    @Test
    public void detectsSourceReplacementEvenWhenBytesAndMtimeAreIdentical() throws Exception {
        sourceMutation(1);
    }

    @Test
    public void detectsSourceRewriteThenRestoreWhenChangeTimeIsAvailable() throws Exception {
        Assume.assumeTrue(support.directory.toPath().getFileSystem().supportedFileAttributeViews().contains("unix"));
        sourceMutation(2);
    }

    @Test
    public void shellLauncherForwardsPathsAndJvmFlags() throws Exception {
        Assume.assumeFalse(System.getProperty("os.name").startsWith("Windows"));
        File snapshot = support.snapshot(ownerTree(false), StreamMode.GZIP, true);
        for (String flags : Arrays.asList("", "-Xms32m -Xmx128m -Dzookeeper.extendedTypesEnabled=true"
                                           + " -Dzookeeper.emulate353TTLNodes=false")) {
            Path output = output();
            support.runLauncher("zkOfflineAudit.sh", flags, "--snapshot-file", snapshot.toString(),
                                "--output-dir", output.toString()).success();
            assertType(output, "/ttl", flags.isEmpty() ? "unknown" : "ttl", flags.isEmpty() ? "unknown" : "ttl");
        }
    }

    private void sourceMutation(int action) throws Exception {
        DataTree tree = new DataTree();
        support.addNode(tree, "/fanout", null);
        for (int i = 0; i < 12000; i++) {
            support.addNode(tree, "/fanout/child-" + i, new byte[1]);
        }
        File snapshot = support.snapshot(tree, StreamMode.CHECKED, true);
        byte[] bytes = Files.readAllBytes(snapshot.toPath());
        FileTime mtime = Files.getLastModifiedTime(snapshot.toPath());
        Path output = output();
        Path log = support.directory.toPath().resolve("mutation-log-" + nextOutput);
        Process process = process(Collections.<String>emptyList(), "--snapshot-file", snapshot.toString(),
                                  "--output-dir", output.toString()).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (!Files.exists(output.resolve("nodes.ndjson")) && process.isAlive() && System.nanoTime() < deadline) {
                Thread.sleep(1);
            }
            assertTrue("Exporter never started writing: " + text(log), Files.exists(output.resolve("nodes.ndjson")));
            if (action == 1) {
                Path replacement = support.directory.toPath().resolve("replacement");
                Files.write(replacement, bytes);
                Files.setLastModifiedTime(replacement, mtime);
                Files.move(replacement, snapshot.toPath(), StandardCopyOption.REPLACE_EXISTING);
            } else {
                bytes[bytes.length - 1] ^= 1;
                Files.write(snapshot.toPath(), bytes);
                if (action == 2) {
                    bytes[bytes.length - 1] ^= 1;
                    Files.write(snapshot.toPath(), bytes);
                }
                Files.setLastModifiedTime(snapshot.toPath(), mtime);
            }
            assertTrue(process.waitFor(30, TimeUnit.SECONDS));
            assertEquals(text(log), 1, process.exitValue());
            assertThat(text(log), containsString("Source changed"));
            assertFalse(Files.exists(output.resolve("manifest.json")));
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                process.waitFor(10, TimeUnit.SECONDS);
            }
        }
    }

    private DataTree customerTree() throws Exception {
        DataTree tree = new DataTree();
        tree.setData("/", new byte[5], 1, 1, 1);
        support.addNode(tree, "/app", new byte[2]);
        support.addNode(tree, "/app/\u00e9", new byte[3]);
        tree.createNode("/app/e", new byte[7], ZooDefs.Ids.OPEN_ACL_UNSAFE, 0x123L, -1, 1, 1);
        support.addNode(tree, "/other", new byte[4]);
        support.addNode(tree, "/zookeeper-client", new byte[11]);
        return tree;
    }

    private DataTree ownerTree(boolean unknown) throws Exception {
        String previous = System.getProperty(EphemeralType.EXTENDED_TYPES_ENABLED_PROPERTY);
        System.setProperty(EphemeralType.EXTENDED_TYPES_ENABLED_PROPERTY, "false");
        try {
            DataTree tree = new DataTree();
            tree.createNode("/session", new byte[5], ZooDefs.Ids.OPEN_ACL_UNSAFE, 0x123L, -1, 1, 1);
            tree.createNode("/second", new byte[7], ZooDefs.Ids.OPEN_ACL_UNSAFE, 0x123L, -1, 1, 1);
            tree.createNode("/container", new byte[2], ZooDefs.Ids.OPEN_ACL_UNSAFE, Long.MIN_VALUE, -1, 1, 1);
            tree.createNode("/ttl", new byte[3], ZooDefs.Ids.OPEN_ACL_UNSAFE, 0xff0000000000002aL, -1, 1, 1);
            tree.createNode("/legacy", new byte[4], ZooDefs.Ids.OPEN_ACL_UNSAFE, 0x800000000000002aL, -1, 1, 1);
            if (unknown) {
                tree.createNode("/unknown", new byte[6], ZooDefs.Ids.OPEN_ACL_UNSAFE, 0xff0001000000002aL, -1, 1, 1);
            }
            return tree;
        } finally {
            if (previous == null) {
                System.clearProperty(EphemeralType.EXTENDED_TYPES_ENABLED_PROPERTY);
            } else {
                System.setProperty(EphemeralType.EXTENDED_TYPES_ENABLED_PROPERTY, previous);
            }
        }
    }

    private void addQuota(DataTree tree, String path, String limits) throws Exception {
        support.addNode(tree, Quotas.quotaZookeeper + path, null);
        support.addNode(tree, Quotas.quotaPath(path), limits.getBytes(StandardCharsets.UTF_8));
        support.addNode(tree, Quotas.statPath(path), "count=0,bytes=0".getBytes(StandardCharsets.UTF_8));
    }

    private Path output() {
        return support.directory.toPath().resolve("export with spaces " + ++nextOutput);
    }

    private SnapshotToolTestSupport.Result run(File snapshot, Path output) throws Exception {
        return run(snapshot, output, Collections.<String>emptyList());
    }

    private SnapshotToolTestSupport.Result run(File snapshot, Path output, List<String> properties,
                                               String... extra) throws Exception {
        List<String> args = new ArrayList<>(Arrays.asList("--snapshot-file", snapshot.toString(),
                                                        "--output-dir", output.toString()));
        args.addAll(Arrays.asList(extra));
        return execute(properties, args.toArray(new String[0]));
    }

    private SnapshotToolTestSupport.Result execute(List<String> properties, String... args) throws Exception {
        Path log = support.directory.toPath().resolve("cli-log-" + ++nextOutput);
        Process process = process(properties, args).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            assertTrue("CLI timed out", process.waitFor(30, TimeUnit.SECONDS));
            return new SnapshotToolTestSupport.Result(process.exitValue(), text(log));
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                process.waitFor(10, TimeUnit.SECONDS);
            }
        }
    }

    private ProcessBuilder process(List<String> properties, String... args) {
        List<String> command = new ArrayList<>(Arrays.asList(
            new File(System.getProperty("java.home"), "bin/java").toString(), "-Xmx128m",
            "-Djava.io.tmpdir=" + System.getProperty("java.io.tmpdir")));
        command.addAll(properties);
        command.addAll(Arrays.asList("-cp",
                                     System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")), MAIN));
        command.addAll(Arrays.asList(args));
        return new ProcessBuilder(command);
    }

    private static void failure(SnapshotToolTestSupport.Result result) {
        assertEquals(result.output, 1, result.exitCode);
        assertThat(result.output, containsString("Unable to export snapshot"));
    }

    private static JsonNode manifest(Path output) throws IOException {
        return JSON.readTree(output.resolve("manifest.json").toFile());
    }

    private static JsonNode node(Path output, String path) throws IOException {
        return record(output.resolve("nodes.ndjson"), "path", path);
    }

    private static JsonNode record(Path file, String key, String value) throws IOException {
        for (JsonNode record : records(file)) {
            if (record.get(key).asText().equals(value)) {
                return record;
            }
        }
        throw new AssertionError("Missing " + value + " in " + file);
    }

    private static List<JsonNode> records(Path file) throws IOException {
        List<JsonNode> records = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                records.add(JSON.readTree(line));
            }
        }
        return records;
    }

    private static void assertType(Path output, String path, String type, String encoding) throws IOException {
        JsonNode record = node(output, path);
        assertEquals(path, type, record.get("node_type").asText());
        assertEquals(path, encoding, record.get("owner_encoding").asText());
    }

    private static Set<String> flags(String... flags) {
        return new HashSet<>(Arrays.asList(flags));
    }

    private static String text(Path path) throws IOException {
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }

    private static String sha256(Path path) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path));
        StringBuilder result = new StringBuilder();
        for (byte value : digest) {
            result.append(String.format("%02x", value & 0xff));
        }
        return result.toString();
    }
}
