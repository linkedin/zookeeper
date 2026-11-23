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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.server.SnapshotComparerTest.SnapshotToolTestSupport;
import org.apache.zookeeper.server.persistence.SnapStream.StreamMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

public class SnapshotRecursiveSummaryTest extends ZKTestCase {

    private SnapshotToolTestSupport support;

    @BeforeEach
    public void setUp() throws IOException {
        support = new SnapshotToolTestSupport();
    }

    @AfterEach
    public void tearDown() throws IOException {
        support.close();
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, Integer.MAX_VALUE})
    public void testRecursiveSubtreeTotalsAndDepth(int depth) throws Exception {
        String output = summarize(tree(), "/app", Integer.toString(depth));
        String expected = " /app\n   children: 5\n   data: 17\n"
                          + "-- /app/branch\n--   children: 2\n--   data: 8";
        if (depth != 1) {
            expected += "\n---- /app/branch/deep\n----   children: 1\n----   data: 5";
        }
        assertEquals(expected, entries(output));
    }

    @Test
    public void testRootIncludesItsOwnData() throws Exception {
        DataTree tree = tree();
        tree.setData("/", new byte[11], 1, 1, 1);

        String output = summarize(tree, "/", "1");

        assertEquals(" /\n   children: 9\n   data: 28\n"
                     + "-- /app\n--   children: 5\n--   data: 17\n"
                     + "-- /zookeeper\n--   children: 2\n--   data: 0", entries(output));
    }

    @Test
    public void testSelectedLeafAndNullData() throws Exception {
        assertEquals("", entries(summarize(tree(), "/app/leaf", "0")));
        DataTree tree = new DataTree();
        support.addNode(tree, "/app", null);
        support.addNode(tree, "/app/leaf", null);

        assertEquals(" /app\n   children: 1\n   data: 0", entries(summarize(tree, "/app", "0")));
    }

    @Test
    public void testEmptySnapshot() throws Exception {
        String output = summarize(new DataTree(), "/", "0");

        assertEquals(" /\n   children: 3\n   data: 0\n"
                     + "-- /zookeeper\n--   children: 2\n--   data: 0", entries(output));
    }

    @Test
    public void testDeepTreeTotalsBeyondOutputLimit() throws Exception {
        DataTree tree = new DataTree();
        support.addNode(tree, "/app", new byte[2]);
        String path = "/app";
        for (int i = 0; i < 64; i++) {
            path += "/n";
            support.addNode(tree, path, new byte[1]);
        }

        assertEquals(" /app\n   children: 64\n   data: 66\n"
                     + "-- /app/n\n--   children: 63\n--   data: 64",
                     entries(summarize(tree, "/app", "1")));
    }

    @Test
    public void testSiblingOrderAndUtf8PayloadBytes() throws Exception {
        DataTree tree = new DataTree();
        support.addNode(tree, "/app", "\u00e9".getBytes(StandardCharsets.UTF_8));
        support.addNode(tree, "/app/z", null);
        support.addNode(tree, "/app/z/leaf", new byte[1]);
        support.addNode(tree, "/app/a", null);
        support.addNode(tree, "/app/a/leaf", new byte[3]);

        assertEquals(" /app\n   children: 4\n   data: 6\n"
                     + "-- /app/a\n--   children: 1\n--   data: 3\n"
                     + "-- /app/z\n--   children: 1\n--   data: 1",
                     entries(summarize(tree, "/app", "0")));
    }

    @Test
    public void testEphemeralDescendantsAreIncluded() throws Exception {
        DataTree tree = new DataTree();
        support.addNode(tree, "/app", new byte[2]);
        tree.createNode("/app/ephemeral", new byte[7], ZooDefs.Ids.OPEN_ACL_UNSAFE, 0x123L, -1, 1, 1);

        assertEquals(" /app\n   children: 1\n   data: 9", entries(summarize(tree, "/app", "0")));
    }

    @ParameterizedTest
    @EnumSource(StreamMode.class)
    public void testCompressedSnapshotsWithAndWithoutDigest(StreamMode mode) throws Exception {
        for (boolean digest : new boolean[]{false, true}) {
            File snapshot = support.snapshot(tree(), mode, digest);
            byte[] before = Files.readAllBytes(snapshot.toPath());

            String output = run(snapshot.toString(), "/app", "1").success();

            assertEquals(" /app\n   children: 5\n   data: 17\n"
                         + "-- /app/branch\n--   children: 2\n--   data: 8", entries(output));
            assertArrayEquals(before, Files.readAllBytes(snapshot.toPath()));
        }
    }

    @Test
    public void testCheckedInSnapshotFixture() throws Exception {
        String output = run(support.fixture("snapshot.2").toString(), "/", "0").success();

        assertEquals(" /\n   children: 3\n   data: 5000\n"
                     + "-- /zookeeper\n--   children: 1\n--   data: 0", entries(output));
    }

    @Test
    public void testInvalidArguments() throws Exception {
        File snapshot = support.snapshot(tree(), StreamMode.CHECKED, true);
        run().invalidInvocation();
        run(snapshot.toString(), "/app").invalidInvocation();
        run(snapshot.toString(), "/app", "0", "extra").invalidInvocation();
        for (String invalid : Arrays.asList("-1", "NaN", "2147483648")) {
            run(snapshot.toString(), "/app", invalid).invalidInvocation();
        }
    }

    @Test
    public void testInvalidZnodePathsAndFiles() throws Exception {
        File snapshot = support.snapshot(tree(), StreamMode.CHECKED, true);
        for (String invalid : Arrays.asList("", "app", "/app/", "/app//leaf", "/app/..", "/missing")) {
            SnapshotToolTestSupport.Result result = run(snapshot.toString(), invalid, "0");
            result.invalidInvocation();
            assertThat(result.output, not(containsString("NullPointerException")));
        }
        run(new File(support.directory, "missing").toString(), "/", "0").invalidInvocation();
        run(support.directory.toString(), "/", "0").invalidInvocation();
    }

    @Test
    public void testRunReportsInvalidInputAndReadFailure() throws Exception {
        SnapshotRecursiveSummary summary = new SnapshotRecursiveSummary();
        File snapshot = support.snapshot(tree(), StreamMode.CHECKED, true);
        assertThrows(IllegalArgumentException.class, () -> summary.run(snapshot.toString(), "/missing", 0));
        assertThrows(IllegalArgumentException.class, () -> summary.run(snapshot.toString(), "/", -1));
        File corrupt = support.corruptSnapshots().get(0);
        assertThrows(IOException.class, () -> summary.run(corrupt.toString(), "/", 0));
    }

    @Test
    public void testCorruptSnapshots() throws Exception {
        for (File corrupt : support.corruptSnapshots()) {
            SnapshotToolTestSupport.Result result = run(corrupt.toString(), "/", "0");
            result.readFailure();
            assertThat(result.output, not(containsString("children:")));
        }
    }

    @Test
    public void testShellLauncher() throws Exception {
        assumeFalse(System.getProperty("os.name").startsWith("Windows"));
        File snapshot = support.snapshot(tree(), StreamMode.GZIP, true);
        for (String flags : Arrays.asList("", "-Xms32m -Xmx128m")) {
            String output = support.runLauncher("zkSnapshotRecursiveSummaryToolkit.sh", flags,
                                               snapshot.toString(), "/app", "1").success();
            assertEquals(" /app\n   children: 5\n   data: 17\n"
                         + "-- /app/branch\n--   children: 2\n--   data: 8", entries(output));
        }
        support.runLauncher("zkSnapshotRecursiveSummaryToolkit.sh", "").invalidInvocation();
    }

    private DataTree tree() throws Exception {
        DataTree tree = new DataTree();
        support.addNode(tree, "/app", new byte[2]);
        support.addNode(tree, "/app/branch", new byte[3]);
        support.addNode(tree, "/app/branch/deep", null);
        support.addNode(tree, "/app/branch/deep/leaf", new byte[5]);
        support.addNode(tree, "/app/empty", null);
        support.addNode(tree, "/app/leaf", new byte[7]);
        return tree;
    }

    private String summarize(DataTree tree, String path, String depth) throws Exception {
        File snapshot = support.snapshot(tree, StreamMode.CHECKED, true);
        return run(snapshot.toString(), path, depth).success();
    }

    private String entries(String output) {
        return Arrays.stream(output.split("\\r?\\n"))
            .filter(line -> line.matches("(--)* (/.*|  (children|data): [0-9]+)"))
            .collect(Collectors.joining("\n"));
    }

    private SnapshotToolTestSupport.Result run(String... args) throws Exception {
        return support.run("org.apache.zookeeper.server.SnapshotRecursiveSummary", "", args);
    }

}
