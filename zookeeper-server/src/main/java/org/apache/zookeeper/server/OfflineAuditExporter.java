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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.jute.BinaryInputArchive;
import org.apache.zookeeper.Quotas;
import org.apache.zookeeper.StatsTrack;
import org.apache.zookeeper.Version;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.StatPersisted;
import org.apache.zookeeper.server.SnapshotComparer.SnapshotData;
import org.apache.zookeeper.server.persistence.SnapStream;
import org.apache.zookeeper.util.ServiceUtils;

/**
 * Streams non-value metadata from one validated snapshot. No transaction logs are
 * replayed and neither the snapshot filename nor its digest establishes capture time
 * or transaction-consistent state. See zookeeperTools.md for the versioned schema.
 */
public final class OfflineAuditExporter {

    private static final int SCHEMA_VERSION = 1;
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Pattern SNAPSHOT_NAME = Pattern.compile("snapshot\\.([0-9a-fA-F]{1,16})(?:\\.(?:gz|snappy))?");
    private static final Pattern QUOTA_VALUE = Pattern.compile("count=-?[0-9]+,bytes=-?[0-9]+");
    private static final Set<String> ACL_SCHEMES = new HashSet<>(Arrays.asList("world", "auth", "digest", "ip", "sasl", "x509"));

    private OfflineAuditExporter() {
    }

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption(Option.builder().longOpt("snapshot-file").hasArg().required().argName("SNAPSHOT")
                          .desc("Read only this snapshot file.").build());
        options.addOption(Option.builder().longOpt("output-dir").hasArg().required().argName("DIRECTORY")
                          .desc("Create a new directory; its parent must already exist.").build());
        options.addOption(Option.builder().longOpt("max-output-bytes").hasArg().argName("BYTES")
                          .desc("Optional positive byte budget, including the manifest. No default policy limit.").build());
        Path source;
        Path output;
        Long limit;
        DecoderSettings decoder;
        try {
            CommandLine command = new DefaultParser().parse(options, args);
            if (!command.getArgList().isEmpty()) {
                throw new IllegalArgumentException("Unexpected positional arguments");
            }
            Set<String> supplied = new HashSet<>();
            for (Option option : command.getOptions()) {
                if (!supplied.add(option.getLongOpt())) {
                    throw new IllegalArgumentException("Repeated option: --" + option.getLongOpt());
                }
            }
            limit = command.hasOption("max-output-bytes")
                ? Long.valueOf(command.getOptionValue("max-output-bytes")) : null;
            if (limit != null && limit <= 0) {
                throw new IllegalArgumentException("max-output-bytes must be a positive 64-bit integer");
            }
            decoder = DecoderSettings.read();
            source = Paths.get(command.getOptionValue("snapshot-file")).toAbsolutePath();
            String error = ZKUtil.validateFileInput(source.toString());
            if (error != null) {
                throw new IllegalArgumentException(error);
            }
            output = Paths.get(command.getOptionValue("output-dir")).toAbsolutePath();
            if (Files.exists(output, LinkOption.NOFOLLOW_LINKS)) {
                throw new IllegalArgumentException("Output already exists; no export attempted and prior files left untouched: " + output);
            }
            Path parent = output.getParent();
            if (parent == null || !Files.isDirectory(parent)) {
                throw new IllegalArgumentException("Output parent must be an existing directory: " + output);
            }
            output = parent.toRealPath().resolve(fileName(output));
        } catch (ParseException | IllegalArgumentException | IOException e) {
            System.err.println(e.getMessage());
            new HelpFormatter().printHelp("zkOfflineAudit.sh", options, true);
            ServiceUtils.requestSystemExit(ExitCode.INVALID_INVOCATION.getValue());
            return;
        }
        try {
            export(source, output, decoder, limit);
            System.out.println("Snapshot-only export complete: " + output);
        } catch (IOException | IllegalArgumentException | ArithmeticException e) {
            System.err.println("Unable to export snapshot: " + e.getMessage());
            ServiceUtils.requestSystemExit(ExitCode.UNEXPECTED_ERROR.getValue());
        }
    }

    /**
     * Returns the entire successful getChildren/getChildren2 reply packet size,
     * including the 16-byte ReplyHeader but excluding the outer four-byte frame.
     */
    public static long childResponseBytes(Iterable<String> children, boolean includeStat) {
        long bytes = includeStat ? 88L : 20L;
        for (String child : children) {
            bytes = Math.addExact(bytes, 4L + child.getBytes(StandardCharsets.UTF_8).length);
        }
        return bytes;
    }

    private static void export(Path source, Path output, DecoderSettings decoder, Long limit) throws IOException {
        SourceIdentity identity = SourceIdentity.read(source);
        SnapshotData snapshot = SnapshotComparer.readSnapshot(identity.path.toFile());
        if (output.getFileSystem().supportedFileAttributeViews().contains("posix")) {
            Files.createDirectory(output, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
        } else {
            Files.createDirectory(output);
        }
        OutputBudget budget = new OutputBudget(limit);
        ExportRecords records = new ExportRecords(snapshot, decoder);
        List<Map<String, Object>> files = new ArrayList<>();
        try (RecordWriter nodes = new RecordWriter(output.resolve("nodes.ndjson"), budget);
             RecordWriter namespaces = new RecordWriter(output.resolve("namespaces.ndjson"), budget);
             RecordWriter sessions = new RecordWriter(output.resolve("sessions.ndjson"), budget)) {
            records.write(nodes, namespaces, sessions);
            files.add(nodes.finish());
            files.add(namespaces.finish());
            files.add(sessions.finish());
        }

        Map<String, Object> manifest = record();
        manifest.put("tool", fields("name", "OfflineAuditExporter", "version", Version.getFullVersion()));
        manifest.put("export_id", UUID.randomUUID().toString());
        manifest.put("export_complete", true);
        manifest.put("recovery_scope", "snapshot-only");
        manifest.put("transaction_logs_replayed", false);
        manifest.put("source_capture_time_ms", null);
        manifest.put("source_capture_provenance", null);
        manifest.put("source_server_version", null);
        manifest.put("source", identity.metadata(snapshot));
        manifest.put("decoder", decoder.metadata());
        manifest.put("snapshot_zxid", snapshotZxid(identity.path));
        manifest.put("last_observed_zxid", hex(records.lastObservedZxid));
        manifest.put("last_observed_zxid_scope", "maximum-node-czxid-mzxid-pzxid-unsigned");
        manifest.put("snapshot_digest", snapshot.digest == null ? null
                     : fields("zxid", hex(snapshot.digest.getZxid()), "version", snapshot.digest.getDigestVersion(),
                              "value", hex(snapshot.digest.getDigest()), "seal_validated", true,
                              "transaction_consistency_verified", false));
        manifest.put("unknown_owner_nodes", records.unknownOwnerNodes);
        manifest.put("max_output_bytes", limit);
        manifest.put("files", files);
        Path pending = output.resolve(".manifest.json.inprogress");
        try (RecordWriter writer = new RecordWriter(pending, budget)) {
            writer.write(manifest);
            writer.finish();
        }
        identity.verify();
        Files.move(pending, output.resolve("manifest.json"), StandardCopyOption.ATOMIC_MOVE);
    }

    private enum NodeType {
        PERSISTENT("persistent", "none"),
        EPHEMERAL("ephemeral", "session"),
        CONTAINER("container", "container"),
        TTL("ttl", "ttl"),
        TTL_353("ttl", "ttl-3.5.3"),
        UNKNOWN("unknown", "unknown");

        final String type;
        final String encoding;

        NodeType(String type, String encoding) {
            this.type = type;
            this.encoding = encoding;
        }
    }

    private static final class DecoderSettings {

        final Boolean extended;
        final Boolean emulate353;
        final int maxBuffer;
        final int extraBuffer;

        DecoderSettings(Boolean extended, Boolean emulate353, int maxBuffer, int extraBuffer) {
            this.extended = extended;
            this.emulate353 = emulate353;
            this.maxBuffer = maxBuffer;
            this.extraBuffer = extraBuffer;
        }

        static DecoderSettings read() {
            Boolean extended = booleanProperty(EphemeralType.EXTENDED_TYPES_ENABLED_PROPERTY).orElse(null);
            Boolean emulate353 = booleanProperty(EphemeralType.TTL_3_5_3_EMULATION_PROPERTY).orElse(null);
            if (Boolean.TRUE.equals(emulate353) && !Boolean.TRUE.equals(extended)) {
                throw new IllegalArgumentException("emulate353TTLNodes=true requires explicit extendedTypesEnabled=true");
            }
            String configuredMax = System.getProperty("jute.maxbuffer");
            if (configuredMax != null && Integer.decode(configuredMax) <= 0) {
                throw new IllegalArgumentException("jute.maxbuffer must be a positive integer");
            }
            int max = BinaryInputArchive.maxBuffer;
            String configuredExtra = System.getProperty("zookeeper.jute.maxbuffer.extrasize");
            int extra = configuredExtra == null ? max : Integer.decode(configuredExtra);
            if (extra < 0) {
                throw new IllegalArgumentException("zookeeper.jute.maxbuffer.extrasize must not be negative");
            }
            extra = Math.max(1024, extra);
            if ((long) max + extra > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Combined Jute buffer limits exceed a signed 32-bit length");
            }
            return new DecoderSettings(extended, emulate353, max, extra);
        }

        NodeType type(long owner) {
            if (owner == 0) {
                return NodeType.PERSISTENT;
            }
            if (owner == EphemeralType.CONTAINER_EPHEMERAL_OWNER) {
                return NodeType.CONTAINER;
            }
            // Negative encodings depend on source properties that are not stored in snapshots.
            if (owner < 0 && (extended == null || (extended && emulate353 == null))) {
                return NodeType.UNKNOWN;
            }
            if (EphemeralType.get(owner) == EphemeralType.TTL) {
                return Boolean.TRUE.equals(emulate353) ? NodeType.TTL_353 : NodeType.TTL;
            }
            return NodeType.EPHEMERAL;
        }

        Map<String, Object> metadata() {
            return fields("source_extended_types_enabled", extended, "source_emulate_353_ttl_nodes", emulate353,
                          "effective_extended_types_enabled", Boolean.TRUE.equals(extended),
                          "effective_emulate_353_ttl_nodes", Boolean.TRUE.equals(emulate353),
                          "jute_maxbuffer", maxBuffer, "jute_extra_maxbuffer", extraBuffer);
        }

        private static Optional<Boolean> booleanProperty(String name) {
            String value = System.getProperty(name);
            if (value == null) {
                return Optional.empty();
            }
            if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
                throw new IllegalArgumentException(name + " must be true or false");
            }
            return Optional.of(Boolean.valueOf(value));
        }
    }

    private static final class ExportRecords {

        final SnapshotData snapshot;
        final DecoderSettings decoder;
        long lastObservedZxid;
        long unknownOwnerNodes;

        ExportRecords(SnapshotData snapshot, DecoderSettings decoder) {
            this.snapshot = snapshot;
            this.decoder = decoder;
        }

        void write(RecordWriter nodes, RecordWriter namespaces, RecordWriter sessions) throws IOException {
            SnapshotRecursiveSummary.walk(snapshot.tree, "/", new SnapshotRecursiveSummary.TreeVisitor() {
                @Override
                public void visit(String path, DataNode node, int depth) throws IOException {
                    nodes.write(nodeRecord(path, node));
                }

                @Override
                public void leave(String path, int depth, long count, long payloadBytes, long pathBytes) throws IOException {
                    if (depth == 0 && count != (long) snapshot.tree.getNodeCount() - 1) {
                        throw new IOException("Snapshot contains unreachable nodes or invalid root aliases");
                    }
                    if (depth == 1 && !Quotas.procZookeeper.equals(path)) {
                        Map<String, Object> namespace = record();
                        namespace.put("path", path);
                        namespace.put("subtree_node_count", count);
                        namespace.put("payload_bytes", payloadBytes);
                        namespace.put("path_utf8_bytes", pathBytes);
                        namespace.putAll(quota(path));
                        namespaces.write(namespace);
                    }
                }
            });
            writeSessions(sessions);
        }

        Map<String, Object> nodeRecord(String path, DataNode node) {
            Map<String, Object> record = record();
            synchronized (node) {
                StatPersisted stat = node.stat;
                long owner = stat.getEphemeralOwner();
                NodeType type = decoder.type(owner);
                if (type == NodeType.UNKNOWN) {
                    unknownOwnerNodes++;
                }
                observe(stat.getCzxid());
                observe(stat.getMzxid());
                observe(stat.getPzxid());
                long responseBytes = childResponseBytes(node.getChildren(), false);
                record.put("path", path);
                record.put("data_length", node.data == null ? 0L : (long) node.data.length);
                record.put("num_children", (long) node.getChildren().size());
                record.put("getchildren_response_bytes", responseBytes);
                record.put("getchildren2_response_bytes", Math.addExact(responseBytes, 68L));
                record.put("ctime_ms", stat.getCtime());
                record.put("mtime_ms", stat.getMtime());
                record.put("czxid", hex(stat.getCzxid()));
                record.put("mzxid", hex(stat.getMzxid()));
                record.put("pzxid", hex(stat.getPzxid()));
                record.put("data_version", stat.getVersion());
                record.put("acl_version", stat.getAversion());
                record.put("persisted_cversion", stat.getCversion());
                record.put("path_utf8_bytes", (long) path.getBytes(StandardCharsets.UTF_8).length);
                record.put("node_type", type.type);
                record.put("ephemeral_owner", hex(owner));
                record.put("owner_encoding", type.encoding);
                record.put("ttl_ms", type == NodeType.TTL || type == NodeType.TTL_353
                           ? EphemeralType.TTL.getValue(owner) : null);
                record.put("acl_risk_flags", aclFlags(snapshot.tree.getACL(node)));
            }
            return record;
        }

        private void observe(long zxid) {
            if (Long.compareUnsigned(zxid, lastObservedZxid) > 0) {
                lastObservedZxid = zxid;
            }
        }

        private Map<String, Object> quota(String path) {
            DataNode limit = snapshot.tree.getNode(Quotas.quotaPath(path));
            boolean statsPresent = snapshot.tree.getNode(Quotas.statPath(path)) != null;
            String status = limit == null ? "absent" : "invalid";
            Integer count = null;
            Long bytes = null;
            String value = null;
            if (limit != null) {
                synchronized (limit) {
                    if (limit.data != null) {
                        value = new String(limit.data, StandardCharsets.UTF_8);
                    }
                }
            }
            if (value != null && QUOTA_VALUE.matcher(value).matches()) {
                try {
                    StatsTrack stats = new StatsTrack(value);
                    if (stats.getCount() >= -1 && stats.getBytes() >= -1) {
                        count = stats.getCount();
                        bytes = stats.getBytes();
                        status = "valid";
                    }
                } catch (NumberFormatException e) {
                    // Malformed quota metadata is a review flag, never a raw-value diagnostic.
                    status = "invalid";
                }
            }
            return fields("quota_status", status, "quota_limit_count", count, "quota_limit_bytes", bytes,
                          "quota_stat_present", statsPresent);
        }

        private void writeSessions(RecordWriter writer) throws IOException {
            for (long owner : snapshot.tree.getSessions()) {
                if (decoder.type(owner) != NodeType.EPHEMERAL) {
                    continue;
                }
                Set<String> paths = snapshot.tree.getEphemerals(owner);
                long payload = 0;
                long pathBytes = 0;
                for (String path : paths) {
                    DataNode node = snapshot.tree.getNode(path);
                    if (node == null) {
                        throw new IOException("Missing ephemeral snapshot node: " + path);
                    }
                    synchronized (node) {
                        payload = Math.addExact(payload, node.data == null ? 0 : node.data.length);
                    }
                    pathBytes = Math.addExact(pathBytes, path.getBytes(StandardCharsets.UTF_8).length);
                }
                Map<String, Object> session = record();
                session.put("session_id", hex(owner));
                session.put("ephemeral_node_count", (long) paths.size());
                session.put("payload_bytes", payload);
                session.put("path_utf8_bytes", pathBytes);
                session.put("present_in_snapshot_session_table", snapshot.sessions.containsKey(owner));
                session.put("timeout_ms", snapshot.sessions.get(owner));
                writer.write(session);
            }
        }
    }

    private static Set<String> aclFlags(List<ACL> acls) {
        Set<String> flags = new LinkedHashSet<>();
        if (acls == null || acls.isEmpty()) {
            flags.add("missing_acl");
            return flags;
        }
        for (ACL acl : acls) {
            Id id = acl.getId();
            int permissions = acl.getPerms();
            if ((permissions & ~ZooDefs.Perms.ALL) != 0) {
                flags.add("invalid_permissions");
            }
            if (id == null || id.getScheme() == null || id.getId() == null) {
                flags.add("invalid_identity");
                continue;
            }
            if ("world".equals(id.getScheme()) && !"anyone".equals(id.getId())) {
                flags.add("invalid_identity");
            }
            if ("world".equals(id.getScheme()) && "anyone".equals(id.getId())) {
                if ((permissions & ZooDefs.Perms.READ) != 0) {
                    flags.add("world_read");
                }
                if ((permissions & (ZooDefs.Perms.WRITE | ZooDefs.Perms.CREATE | ZooDefs.Perms.DELETE)) != 0) {
                    flags.add("world_write");
                }
                if ((permissions & ZooDefs.Perms.ADMIN) != 0) {
                    flags.add("world_admin");
                }
            }
            if ("auth".equals(id.getScheme())) {
                flags.add("auth");
            }
            if (!ACL_SCHEMES.contains(id.getScheme())) {
                flags.add("unknown_scheme");
            }
        }
        return flags;
    }

    private static final class SourceIdentity {

        final Path selected;
        final Path path;
        final BasicFileAttributes attributes;
        final FileTime changeTime;
        final String sha256;

        SourceIdentity(Path selected, Path path, BasicFileAttributes attributes, FileTime changeTime, String sha256) {
            this.selected = selected;
            this.path = path;
            this.attributes = attributes;
            this.changeTime = changeTime;
            this.sha256 = sha256;
        }

        static SourceIdentity read(Path selected) throws IOException {
            Path path = selected.toRealPath();
            BasicFileAttributes before = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            FileTime changeTime = changeTime(path);
            if (!before.isRegularFile()) {
                throw new IOException("Snapshot must be a regular file: " + selected);
            }
            MessageDigest digest = sha256();
            try (InputStream input = Files.newInputStream(path)) {
                byte[] buffer = new byte[65536];
                int count;
                while ((count = input.read(buffer)) != -1) {
                    digest.update(buffer, 0, count);
                }
            }
            BasicFileAttributes after = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!path.equals(selected.toRealPath()) || !sameAttributes(before, after)
                || !Objects.equals(changeTime, changeTime(path))) {
                throw new IOException("Source changed while hashing: " + selected);
            }
            return new SourceIdentity(selected, path, before, changeTime, hexBytes(digest.digest()));
        }

        void verify() throws IOException {
            SourceIdentity current = read(selected);
            if (!path.equals(current.path) || !sameAttributes(attributes, current.attributes)
                || !Objects.equals(changeTime, current.changeTime) || !sha256.equals(current.sha256)) {
                throw new IOException("Source changed during export: " + selected);
            }
        }

        Map<String, Object> metadata(SnapshotData snapshot) {
            return fields("path", path.toString(), "sha256", sha256, "size_bytes", attributes.size(),
                          "mtime_ms", attributes.lastModifiedTime().toMillis(),
                          "change_time_ms", changeTime == null ? null : changeTime.toMillis(),
                          "compression", SnapStream.getStreamMode(fileName(path)).name().toLowerCase(Locale.ROOT),
                          "format_version", snapshot.header.getVersion(), "database_id", hex(snapshot.header.getDbid()));
        }

        private static FileTime changeTime(Path path) throws IOException {
            return path.getFileSystem().supportedFileAttributeViews().contains("unix")
                ? (FileTime) Files.getAttribute(path, "unix:ctime", LinkOption.NOFOLLOW_LINKS) : null;
        }

        private static boolean sameAttributes(BasicFileAttributes left, BasicFileAttributes right) {
            return right.isRegularFile() && left.size() == right.size()
                && left.lastModifiedTime().equals(right.lastModifiedTime())
                && left.creationTime().equals(right.creationTime())
                && Objects.equals(left.fileKey(), right.fileKey());
        }
    }

    private static final class OutputBudget {

        final Long limit;
        long written;

        OutputBudget(Long limit) {
            this.limit = limit;
        }

        void reserve(long count) throws IOException {
            long next = Math.addExact(written, count);
            if (limit != null && next > limit) {
                throw new IOException("Output byte budget exceeded (max-output-bytes=" + limit + ")");
            }
            written = next;
        }
    }

    private static final class RecordWriter implements Closeable {

        final Path path;
        final OutputBudget budget;
        final MessageDigest digest = sha256();
        final FileChannel channel;
        final OutputStream output;
        long records;
        long bytes;

        RecordWriter(Path path, OutputBudget budget) throws IOException {
            this.path = path;
            this.budget = budget;
            channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            output = new BufferedOutputStream(new DigestOutputStream(Channels.newOutputStream(channel), digest));
        }

        void write(Map<String, Object> record) throws IOException {
            byte[] encoded = JSON.writeValueAsBytes(record);
            long size = (long) encoded.length + 1;
            budget.reserve(size);
            output.write(encoded);
            output.write('\n');
            bytes = Math.addExact(bytes, size);
            records++;
        }

        Map<String, Object> finish() throws IOException {
            output.flush();
            channel.force(true);
            return fields("name", fileName(path), "records", records,
                          "size_bytes", bytes, "sha256", hexBytes(digest.digest()));
        }

        @Override
        public void close() throws IOException {
            output.close();
        }
    }

    private static String snapshotZxid(Path source) {
        Matcher matcher = SNAPSHOT_NAME.matcher(fileName(source));
        return matcher.matches() ? hex(Long.parseUnsignedLong(matcher.group(1), 16)) : null;
    }

    private static String fileName(Path path) {
        Path name = path.getFileName();
        if (name == null) {
            throw new IllegalArgumentException("Path must identify a file: " + path);
        }
        return name.toString();
    }

    private static Map<String, Object> record() {
        return fields("schema_version", SCHEMA_VERSION);
    }

    private static Map<String, Object> fields(Object... fields) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < fields.length; i += 2) {
            result.put((String) fields[i], fields[i + 1]);
        }
        return result;
    }

    private static String hex(long value) {
        return String.format(Locale.ROOT, "0x%016x", value);
    }

    private static String hexBytes(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            result.append(String.format(Locale.ROOT, "%02x", value & 0xff));
        }
        return result.toString();
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JVM does not provide SHA-256", e);
        }
    }
}
