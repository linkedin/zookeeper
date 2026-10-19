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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.jute.BinaryInputArchive;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.server.persistence.FileSnap;
import org.apache.zookeeper.server.persistence.SnapStream;
import org.apache.zookeeper.server.persistence.SnapStream.StreamMode;
import org.apache.zookeeper.test.ClientBase;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class SnapshotComparerTest extends ZKTestCase {

    private SnapshotToolTestSupport support;

    @Before
    public void setUp() throws IOException {
        support = new SnapshotToolTestSupport();
    }

    @After
    public void tearDown() throws IOException {
        support.close();
    }

    @Test
    public void testAddedAndDeletedSubtrees() throws Exception {
        DataTree left = new DataTree();
        support.addNode(left, "/app", new byte[1]);
        support.addNode(left, "/app/removed", new byte[2]);
        support.addNode(left, "/app/removed/leaf", new byte[3]);
        DataTree right = new DataTree();
        support.addNode(right, "/app", new byte[1]);
        support.addNode(right, "/app/added", new byte[4]);
        support.addNode(right, "/app/added/a", new byte[5]);
        support.addNode(right, "/app/added/b", null);

        String output = compare(left, right, "0", "0", "-d");

        assertThat(output, containsString("Node  found in both trees. Delta: 4 bytes, 1 descendants"));
        assertThat(output, containsString("Node /app found in both trees. Delta: 4 bytes, 1 descendants"));
        assertThat(output, containsString("Node /app/removed found only in left tree. Descendant size: 5. Descendant count: 1"));
        assertThat(output, containsString("Node /app/removed/leaf found only in left tree. Descendant size: 3. Descendant count: 0"));
        assertThat(output, containsString("Node /app/added found only in right tree. Descendant size: 9. Descendant count: 2"));
        assertThat(output, containsString("Node /app/added/a found only in right tree. Descendant size: 5. Descendant count: 0"));
        assertThat(output, containsString("Filtered right node /app/added/b of size 0"));
        assertTrue(output, output.indexOf("Node /app/added found") < output.indexOf("Node /app/removed found"));
        assertThat(output, containsString("All layers compared."));
    }

    @Test
    public void testSignedByteDeltas() throws Exception {
        DataTree left = new DataTree();
        support.addNode(left, "/grow", new byte[1]);
        support.addNode(left, "/shrink", new byte[4]);
        DataTree right = new DataTree();
        support.addNode(right, "/grow", new byte[4]);
        support.addNode(right, "/shrink", new byte[1]);

        String output = compare(left, right, "0", "2147483647");

        assertThat(output, containsString("Node /grow found in both trees. Delta: 3 bytes, 0 descendants"));
        assertThat(output, containsString("Node /shrink found in both trees. Delta: -3 bytes, 0 descendants"));
    }

    @Test
    public void testByteThresholdIsExclusive() throws Exception {
        DataTree left = new DataTree();
        support.addNode(left, "/node", new byte[1]);
        DataTree right = new DataTree();
        support.addNode(right, "/node", new byte[5]);

        String filtered = compare(left, right, "4", "0", "--debug");
        String included = compare(left, right, "3", "0");

        assertThat(filtered, containsString("Filtered node /node of left size 1, right size 5"));
        assertThat(filtered, not(containsString("Node /node found")));
        assertThat(included, containsString("Node /node found in both trees. Delta: 4 bytes, 0 descendants"));
    }

    @Test
    public void testDescendantThresholdWithNullData() throws Exception {
        DataTree left = new DataTree();
        support.addNode(left, "/app", null);
        DataTree right = new DataTree();
        support.addNode(right, "/app", null);
        support.addNode(right, "/app/child", null);

        String filtered = compare(left, right, "2147483647", "1", "-d");
        String included = compare(left, right, "2147483647", "0");
        String removed = compare(right, left, "2147483647", "0");

        assertThat(filtered, containsString("Filtered node /app of left size 0, right size 0"));
        assertThat(filtered, not(containsString("Node /app found")));
        assertThat(included, containsString("Node /app found in both trees. Delta: 0 bytes, 1 descendants"));
        assertThat(removed, containsString("Node /app found in both trees. Delta: 0 bytes, -1 descendants"));
    }

    @Test
    public void testEphemeralNodesAreIncluded() throws Exception {
        DataTree right = new DataTree();
        right.createNode("/ephemeral", new byte[7], ZooDefs.Ids.OPEN_ACL_UNSAFE, 0x123L, -1, 1, 1);

        String output = compare(new DataTree(), right, "0", "0");

        assertThat(output, containsString("Node /ephemeral found only in right tree. Descendant size: 7. Descendant count: 0"));
        assertThat(output, containsString("Node  found in both trees. Delta: 7 bytes, 1 descendants"));
    }

    @Test
    public void testEqualLengthPayloadsAreOnlySizeCompared() throws Exception {
        DataTree left = new DataTree();
        support.addNode(left, "/equal", "aa".getBytes(StandardCharsets.UTF_8));
        DataTree right = new DataTree();
        support.addNode(right, "/equal", "bb".getBytes(StandardCharsets.UTF_8));

        String output = compare(left, right, "0", "0", "-d");

        assertThat(output, containsString("Filtered node /equal of left size 2, right size 2"));
        assertThat(output, not(containsString("Node /equal found")));
        assertThat(output, containsString("All layers compared."));
    }

    @Test
    public void testMixedCompressionFormats() throws Exception {
        DataTree leftTree = new DataTree();
        support.addNode(leftTree, "/node", new byte[1]);
        DataTree rightTree = new DataTree();
        support.addNode(rightTree, "/node", new byte[3]);
        File left = support.snapshot(leftTree, StreamMode.CHECKED, true);
        for (StreamMode mode : StreamMode.values()) {
            File right = support.snapshot(rightTree, mode, true);
            String output = run("", arguments(left, right, "0", "0")).success();
            assertThat(output, containsString("Node /node found in both trees. Delta: 2 bytes, 0 descendants"));
        }
    }

    @Test
    public void testSnapshotsWithoutDigest() throws Exception {
        DataTree rightTree = new DataTree();
        support.addNode(rightTree, "/node", new byte[3]);
        File left = support.snapshot(new DataTree(), StreamMode.CHECKED, false);
        File right = support.snapshot(rightTree, StreamMode.CHECKED, false);

        String output = run("", arguments(left, right, "0", "0")).success();

        assertThat(output, containsString("Node /node found only in right tree. Descendant size: 3. Descendant count: 0"));
    }

    @Test
    public void testInteractivePathsAndDepths() throws Exception {
        DataTree leftTree = new DataTree();
        support.addNode(leftTree, "/app", null);
        support.addNode(leftTree, "/app/leaf", new byte[1]);
        DataTree rightTree = new DataTree();
        support.addNode(rightTree, "/app", null);
        support.addNode(rightTree, "/app/leaf", new byte[3]);
        File left = support.snapshot(leftTree, StreamMode.CHECKED, true);
        File right = support.snapshot(rightTree, StreamMode.CHECKED, true);

        String output = run("/\n/app\n/app/leaf\n/missing\n-1\n999\nbad\n1\n\n\n",
                            arguments(left, right, "0", "0", "--interactive")).success();

        assertThat(output, containsString("Analysis for node /"));
        assertThat(output, not(containsString("Path / is neither found")));
        assertThat(output, containsString("Node /app found in both trees. Delta: 2 bytes, 0 descendants"));
        assertThat(output, containsString("Node /app/leaf found in both trees. Delta: 2 bytes, 0 descendants"));
        assertThat(output, not(containsString("Path /app/leaf is neither found")));
        assertThat(output, containsString("Path /missing is neither found in left tree nor right tree."));
        assertThat(output, containsString("Depth must be in range [0, 2]"));
        assertThat(output, containsString("Input bad is not valid."));
        assertThat(output, containsString("Analysis for depth 1"));
        assertThat(output, containsString("All layers compared."));
    }

    @Test
    public void testInteractiveEndOfInput() throws Exception {
        File snapshot = support.snapshot(new DataTree(), StreamMode.CHECKED, true);

        String output = run("", arguments(snapshot, snapshot, "0", "0", "-i")).success();

        assertThat(output, containsString("End of input."));
        assertThat(output, not(containsString("All layers compared.")));
    }

    @Test
    public void testInvalidArguments() throws Exception {
        File snapshot = support.snapshot(new DataTree(), StreamMode.CHECKED, true);
        run("").invalidInvocation();
        run("", "-l", snapshot.toString()).invalidInvocation();
        for (String invalid : Arrays.asList("-1", "NaN", "2147483648")) {
            run("", arguments(snapshot, snapshot, invalid, "0")).invalidInvocation();
            run("", arguments(snapshot, snapshot, "0", invalid)).invalidInvocation();
        }
        run("", arguments(snapshot, snapshot, "0", "0", "--unknown")).invalidInvocation();
        run("", arguments(snapshot, snapshot, "0", "0", "extra")).invalidInvocation();
    }

    @Test
    public void testInvalidFiles() throws Exception {
        File snapshot = support.snapshot(new DataTree(), StreamMode.CHECKED, true);
        File missing = new File(support.directory, "missing");
        for (File invalid : Arrays.asList(missing, support.directory)) {
            run("", arguments(invalid, snapshot, "0", "0")).invalidInvocation();
            run("", arguments(snapshot, invalid, "0", "0")).invalidInvocation();
        }
    }

    @Test
    public void testCorruptSnapshots() throws Exception {
        File valid = support.snapshot(new DataTree(), StreamMode.CHECKED, true);
        for (File corrupt : support.corruptSnapshots()) {
            SnapshotToolTestSupport.Result result = run("", arguments(corrupt, valid, "0", "0"));
            result.readFailure();
            assertThat(result.output, not(containsString("All layers compared.")));
        }
    }

    @Test
    public void testShellLauncher() throws Exception {
        Assume.assumeFalse(System.getProperty("os.name").startsWith("Windows"));
        DataTree rightTree = new DataTree();
        support.addNode(rightTree, "/node", new byte[3]);
        File left = support.snapshot(new DataTree(), StreamMode.CHECKED, true);
        File right = support.snapshot(rightTree, StreamMode.GZIP, true);
        for (String flags : Arrays.asList("", "-Xms32m -Xmx128m")) {
            String output = support.runLauncher("zkSnapshotComparer.sh", flags,
                                               "-l", left.toString(), "-r", right.toString(), "-b", "0", "-n", "0").success();
            assertThat(output, containsString("Node /node found only in right tree. Descendant size: 3. Descendant count: 0"));
        }
    }

    private String compare(DataTree left, DataTree right, String bytes, String nodes, String... flags) throws Exception {
        return run("", arguments(support.snapshot(left, StreamMode.CHECKED, true),
                                 support.snapshot(right, StreamMode.CHECKED, true),
                                 bytes, nodes, flags)).success();
    }

    private String[] arguments(File left, File right, String bytes, String nodes, String... flags) {
        List<String> args = new ArrayList<>(Arrays.asList("--left", left.toString(), "--right", right.toString(),
                                                        "--bytes", bytes, "--nodes", nodes));
        args.addAll(Arrays.asList(flags));
        return args.toArray(new String[0]);
    }

    private SnapshotToolTestSupport.Result run(String input, String... args) throws Exception {
        return support.run("org.apache.zookeeper.server.SnapshotComparer", input, args);
    }

    static class SnapshotToolTestSupport {

        final File directory = ClientBase.createEmptyTestDir();
        private int nextFile;

        SnapshotToolTestSupport() throws IOException {
        }

        void addNode(DataTree tree, String path, byte[] data) throws Exception {
            tree.createNode(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, 0, -1, 1, 1);
        }

        File snapshot(DataTree tree, StreamMode mode, boolean digest) throws IOException {
            File snapshots = new File(directory, "snapshots with spaces");
            assertTrue(snapshots.isDirectory() || snapshots.mkdir());
            File file = new File(snapshots, "snapshot." + Integer.toHexString(++nextFile) + mode.getFileExtension());
            StreamMode previousMode = SnapStream.getStreamMode();
            boolean previousDigest = ZooKeeperServer.isDigestEnabled();
            try {
                SnapStream.setStreamMode(mode);
                ZooKeeperServer.setDigestEnabled(digest);
                new FileSnap(null).serialize(tree, Collections.singletonMap(0x123L, 30000), file, false);
            } finally {
                SnapStream.setStreamMode(previousMode);
                ZooKeeperServer.setDigestEnabled(previousDigest);
            }
            return file;
        }

        List<File> corruptSnapshots() throws IOException {
            List<File> files = new ArrayList<>();
            File badHeader = snapshot(new DataTree(), StreamMode.CHECKED, true);
            byte[] bytes = Files.readAllBytes(badHeader.toPath());
            bytes[0] ^= 1;
            Files.write(badHeader.toPath(), bytes);
            files.add(badHeader);

            File truncated = snapshot(new DataTree(), StreamMode.CHECKED, true);
            bytes = Files.readAllBytes(truncated.toPath());
            Files.write(truncated.toPath(), Arrays.copyOf(bytes, bytes.length / 2));
            files.add(truncated);

            File checksum = snapshot(new DataTree(), StreamMode.CHECKED, true);
            bytes = Files.readAllBytes(checksum.toPath());
            ByteArrayInputStream input = new ByteArrayInputStream(bytes);
            new FileSnap(null).deserialize(new DataTree(), new HashMap<Long, Integer>(),
                                           BinaryInputArchive.getArchive(input));
            int sealOffset = bytes.length - input.available();
            bytes[sealOffset] ^= 1;
            Files.write(checksum.toPath(), bytes);
            files.add(checksum);

            File partialDigest = snapshot(new DataTree(), StreamMode.CHECKED, true);
            bytes = Files.readAllBytes(partialDigest.toPath());
            Files.write(partialDigest.toPath(), Arrays.copyOf(bytes, sealOffset + 14));
            files.add(partialDigest);

            File digestSeal = snapshot(new DataTree(), StreamMode.CHECKED, true);
            bytes = Files.readAllBytes(digestSeal.toPath());
            bytes[bytes.length - 13] ^= 1;
            Files.write(digestSeal.toPath(), bytes);
            files.add(digestSeal);

            File gzipTrailer = snapshot(new DataTree(), StreamMode.GZIP, true);
            bytes = Files.readAllBytes(gzipTrailer.toPath());
            bytes[bytes.length - 1] ^= 1;
            Files.write(gzipTrailer.toPath(), bytes);
            files.add(gzipTrailer);
            return files;
        }

        Result run(String mainClass, String input, String... args) throws Exception {
            List<String> command = new ArrayList<>(Arrays.asList(
                new File(System.getProperty("java.home"), "bin/java").toString(),
                "-Xmx128m",
                "-Djava.io.tmpdir=" + System.getProperty("java.io.tmpdir"),
                "-cp",
                System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                mainClass));
            command.addAll(Arrays.asList(args));
            return runProcess(new ProcessBuilder(command), input);
        }

        Result runLauncher(String launcher, String flags, String... args) throws Exception {
            File script = new File(System.getProperty("basedir", "."), "../bin/" + launcher);
            List<String> command = new ArrayList<>(Arrays.asList("bash", script.getCanonicalPath()));
            command.addAll(Arrays.asList(args));
            ProcessBuilder builder = new ProcessBuilder(command);
            builder.environment().put("JAVA_HOME", System.getProperty("java.home"));
            builder.environment().put("CLASSPATH",
                                      System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")));
            builder.environment().put("JVMFLAGS", flags);
            return runProcess(builder, "");
        }

        private Result runProcess(ProcessBuilder builder, String input) throws Exception {
            File outputFile = new File(directory, "output-" + ++nextFile);
            File inputFile = new File(directory, "input-" + nextFile);
            Files.write(inputFile.toPath(), input.getBytes(StandardCharsets.UTF_8));
            Process process = builder
                .redirectInput(inputFile)
                .redirectErrorStream(true)
                .redirectOutput(outputFile)
                .start();
            try {
                assertTrue("CLI did not exit: " + builder.command(), process.waitFor(30, TimeUnit.SECONDS));
                return new Result(process.exitValue(),
                                  new String(Files.readAllBytes(outputFile.toPath()), StandardCharsets.UTF_8));
            } finally {
                if (process.isAlive()) {
                    process.destroyForcibly();
                }
            }
        }

        void close() throws IOException {
            FileUtils.deleteDirectory(directory);
        }

        static class Result {

            final int exitCode;
            final String output;

            Result(int exitCode, String output) {
                this.exitCode = exitCode;
                this.output = output;
            }

            String success() {
                assertEquals(output, ExitCode.EXECUTION_FINISHED.getValue(), exitCode);
                return output;
            }

            void invalidInvocation() {
                assertEquals(output, ExitCode.INVALID_INVOCATION.getValue(), exitCode);
                assertThat(output.toLowerCase(), containsString("usage:"));
            }

            void readFailure() {
                assertEquals(output, ExitCode.UNEXPECTED_ERROR.getValue(), exitCode);
                assertThat(output, containsString("Unable to read snapshot"));
            }
        }
    }

}
