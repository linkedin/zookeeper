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
import static org.junit.Assert.assertThat;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.server.SnapshotComparerTest.SnapshotToolTestSupport;
import org.apache.zookeeper.server.persistence.SnapStream.StreamMode;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class SnapshotRecursiveSummaryTest extends ZKTestCase {

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
    public void testRecursiveSubtreeTotals() throws Exception {
        String output = summarize(tree(), "/app", "0");

        assertThat(output, containsString(" /app\n   children: 5\n   data: 17"));
        assertThat(output, containsString("-- /app/branch\n--   children: 2\n--   data: 8"));
        assertThat(output, containsString("---- /app/branch/deep\n----   children: 1\n----   data: 5"));
        assertThat(output, not(containsString(" /app/leaf\n")));
        assertThat(output, not(containsString(" /app/empty\n")));
        assertThat(output, not(containsString(" /zookeeper\n")));
    }

    @Test
    public void testDepthOnlyLimitsOutput() throws Exception {
        String output = summarize(tree(), "/app", "1");

        assertThat(output, containsString(" /app\n   children: 5\n   data: 17"));
        assertThat(output, containsString("-- /app/branch\n--   children: 2\n--   data: 8"));
        assertThat(output, not(containsString(" /app/branch/deep\n")));
    }

    @Test
    public void testRootIncludesItsOwnData() throws Exception {
        DataTree tree = tree();
        tree.setData("/", new byte[11], 1, 1, 1);

        String output = summarize(tree, "/", "1");

        assertThat(output, containsString(" /\n   children: 9\n   data: 28"));
        assertThat(output, containsString("-- /app\n--   children: 5\n--   data: 17"));
        assertThat(output, containsString("-- /zookeeper\n--   children: 2\n--   data: 0"));
    }

    @Test
    public void testSelectedLeafIsNotPrinted() throws Exception {
        String output = summarize(tree(), "/app/leaf", "0");

        assertThat(output, not(containsString("children:")));
        assertThat(output, not(containsString("data:")));
    }

    @Test
    public void testNullDataContributesZeroBytes() throws Exception {
        DataTree tree = new DataTree();
        support.addNode(tree, "/app", null);
        support.addNode(tree, "/app/leaf", null);

        String output = summarize(tree, "/app", "0");

        assertThat(output, containsString(" /app\n   children: 1\n   data: 0"));
    }

    @Test
    public void testEphemeralDescendantsAreIncluded() throws Exception {
        DataTree tree = new DataTree();
        support.addNode(tree, "/app", new byte[2]);
        tree.createNode("/app/ephemeral", new byte[7], ZooDefs.Ids.OPEN_ACL_UNSAFE, 0x123L, -1, 1, 1);

        String output = summarize(tree, "/app", "0");

        assertThat(output, containsString(" /app\n   children: 1\n   data: 9"));
    }

    @Test
    public void testCompressedSnapshots() throws Exception {
        for (StreamMode mode : StreamMode.values()) {
            File snapshot = support.snapshot(tree(), mode, true);
            String output = run(snapshot.toString(), "/app", "0").success();
            assertThat(output, containsString(" /app\n   children: 5\n   data: 17"));
        }
    }

    @Test
    public void testSnapshotWithoutDigest() throws Exception {
        File snapshot = support.snapshot(tree(), StreamMode.CHECKED, false);

        String output = run(snapshot.toString(), "/app", "0").success();

        assertThat(output, containsString(" /app\n   children: 5\n   data: 17"));
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
    public void testInvalidZnodePaths() throws Exception {
        File snapshot = support.snapshot(tree(), StreamMode.CHECKED, true);
        for (String invalid : Arrays.asList("", "app", "/app/", "/app//leaf", "/app/..", "/missing")) {
            SnapshotToolTestSupport.Result result = run(snapshot.toString(), invalid, "0");
            result.invalidInvocation();
            assertThat(result.output, not(containsString("NullPointerException")));
        }
    }

    @Test
    public void testInvalidFiles() throws Exception {
        run(new File(support.directory, "missing").toString(), "/", "0").invalidInvocation();
        run(support.directory.toString(), "/", "0").invalidInvocation();
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
        Assume.assumeFalse(System.getProperty("os.name").startsWith("Windows"));
        File snapshot = support.snapshot(tree(), StreamMode.GZIP, true);
        for (String flags : Arrays.asList("", "-Xms32m -Xmx128m")) {
            String output = support.runLauncher("zkSnapshotRecursiveSummaryToolkit.sh", flags,
                                               snapshot.toString(), "/app", "0").success();
            assertThat(output, containsString(" /app\n   children: 5\n   data: 17"));
        }
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

    private SnapshotToolTestSupport.Result run(String... args) throws Exception {
        return support.run("org.apache.zookeeper.server.SnapshotRecursiveSummary", "", args);
    }

}
