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

package org.apache.zookeeper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.jute.BinaryInputArchive;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.test.ClientBase;
import org.junit.BeforeClass;
import org.junit.Test;

public class ZKUtilTest extends ClientBase {

    private static final File testData = new File(System.getProperty("test.data.dir", "build/test/data"));

    @BeforeClass
    public static void init() {
        testData.mkdirs();
    }

    @Test
    public void testValidateFileInput() throws IOException {
        File file = File.createTempFile("test", ".junit", testData);
        file.deleteOnExit();
        String absolutePath = file.getAbsolutePath();
        String error = ZKUtil.validateFileInput(absolutePath);
        assertNull(error);
    }

    @Test
    public void testValidateFileInputNotExist() {
        String fileName = UUID.randomUUID().toString();
        File file = new File(testData, fileName);
        String absolutePath = file.getAbsolutePath();
        String error = ZKUtil.validateFileInput(absolutePath);
        assertNotNull(error);
        String expectedMessage = "File '" + absolutePath + "' does not exist.";
        assertEquals(expectedMessage, error);
    }

    @Test
    public void testValidateFileInputDirectory() throws Exception {
        File file = File.createTempFile("test", ".junit", testData);
        file.deleteOnExit();
        // delete file, as we need directory not file
        file.delete();
        file.mkdir();
        String absolutePath = file.getAbsolutePath();
        String error = ZKUtil.validateFileInput(absolutePath);
        assertNotNull(error);
        String expectedMessage = "'" + absolutePath + "' is a direcory. it must be a file.";
        assertEquals(expectedMessage, error);
    }

    @Test
    public void testUnreadableFileInput() throws Exception {
        //skip this test on Windows, coverage on Linux
        assumeTrue(!org.apache.zookeeper.Shell.WINDOWS);
        File file = File.createTempFile("test", ".junit", testData);
        file.setReadable(false, false);
        file.deleteOnExit();
        String absolutePath = file.getAbsolutePath();
        String error = ZKUtil.validateFileInput(absolutePath);
        assertNotNull(error);
        String expectedMessage = "Read permission is denied on the file '" + absolutePath + "'";
        assertEquals(expectedMessage, error);
    }

    @Test
    public void testDeleteRecursiveInAsyncMode() throws Exception {
        int batchSize = 10;
        testDeleteRecursiveInSyncAsyncMode(batchSize);
    }

    @Test
    public void testDeleteRecursiveInSyncMode() throws Exception {
        int batchSize = 0;
        testDeleteRecursiveInSyncAsyncMode(batchSize);
    }

    // batchSize>0 is async mode otherwise it is sync mode
    private void testDeleteRecursiveInSyncAsyncMode(int batchSize)
        throws IOException, InterruptedException, KeeperException {
        TestableZooKeeper zk = createClient();
        String parentPath = "/a";
        zk.create(parentPath, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        int numberOfNodes = 50;
        List<Op> ops = new ArrayList<>();
        for (int i = 0; i < numberOfNodes; i++) {
            ops.add(Op.create(parentPath + "/a" + i, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT));
        }
        zk.multi(ops);
        ops.clear();

        // check nodes create successfully
        List<String> children = zk.getChildren(parentPath, false);
        assertEquals(numberOfNodes, children.size());

        // create one more level of z nodes
        String subNode = "/a/a0";
        for (int i = 0; i < numberOfNodes; i++) {
            ops.add(Op.create(subNode + "/b" + i, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT));
        }
        zk.multi(ops);

        // check sub nodes created successfully
        children = zk.getChildren(subNode, false);
        assertEquals(numberOfNodes, children.size());

        ZKUtil.deleteRecursive(zk, parentPath, batchSize);
        Stat exists = zk.exists(parentPath, false);
        assertNull("ZKUtil.deleteRecursive() could not delete all the z nodes", exists);
    }

    @Test
    public void testDeleteChildrenRecursiveKeepsRootInAsyncMode() throws Exception {
        testDeleteChildrenRecursiveKeepsRoot(10);
    }

    @Test
    public void testDeleteChildrenRecursiveKeepsRootInSyncMode() throws Exception {
        testDeleteChildrenRecursiveKeepsRoot(0);
    }

    private void testDeleteChildrenRecursiveKeepsRoot(int batchSize) throws Exception {
        TestableZooKeeper zk = createClient();
        String parentPath = "/keep";
        byte[] data = "payload".getBytes();
        zk.create(parentPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        List<Op> ops = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            ops.add(Op.create(parentPath + "/c" + i, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT));
        }
        zk.multi(ops);
        zk.create(parentPath + "/c0/nested", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        assertTrue(ZKUtil.deleteChildrenRecursive(zk, parentPath, batchSize));

        assertNotNull("root must be kept", zk.exists(parentPath, false));
        assertEquals(0, zk.getChildren(parentPath, false).size());
        assertEquals("root data must be untouched", new String(data), new String(zk.getData(parentPath, false, null)));
    }

    @Test
    public void testDeleteChildrenRecursiveOnChildlessRoot() throws Exception {
        TestableZooKeeper zk = createClient();
        zk.create("/empty", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        assertTrue(ZKUtil.deleteChildrenRecursive(zk, "/empty", 10));
        assertTrue(ZKUtil.deleteChildrenRecursive(zk, "/empty", 0));

        assertNotNull(zk.exists("/empty", false));
    }

    @Test(timeout = 120000)
    public void testDeleteChildrenRecursiveBeyondJuteMaxBuffer() throws Exception {
        TestableZooKeeper zk = createClient();
        String parentPath = "/huge";
        zk.create(parentPath, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        // enough children for a plain getChildren response to exceed jute.maxbuffer
        int numChildren = BinaryInputArchive.maxBuffer / UUID.randomUUID().toString().length() + 1;
        for (int i = 0; i < numChildren; i += 1000) {
            List<Op> ops = new ArrayList<>();
            for (int j = i; j < i + 1000 && j < numChildren; j++) {
                ops.add(Op.create(parentPath + "/" + UUID.randomUUID(), "".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
            }
            zk.multi(ops);
        }

        assertTrue(ZKUtil.deleteChildrenRecursive(zk, parentPath, 1000));

        assertNotNull(zk.exists(parentPath, false));
        assertEquals(0, zk.getChildren(parentPath, false).size());
    }

    @Test
    public void testDeleteChildrenRecursiveBatchBoundaries() throws Exception {
        TestableZooKeeper zk = createClient();
        // tree of 1 root + 5 children + 1 grandchild = 6 descendants
        // batch sizes: 1, divides evenly (2, 3, 6), does not divide evenly (4), larger than tree (100)
        for (int batchSize : new int[] {1, 2, 3, 4, 6, 100}) {
            String root = "/boundary" + batchSize;
            zk.create(root, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            for (int i = 0; i < 5; i++) {
                zk.create(root + "/c" + i, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            zk.create(root + "/c0/g", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

            assertTrue("batchSize=" + batchSize, ZKUtil.deleteChildrenRecursive(zk, root, batchSize));

            assertNotNull("root must be kept, batchSize=" + batchSize, zk.exists(root, false));
            assertEquals("batchSize=" + batchSize, 0, zk.getChildren(root, false).size());
        }
    }

    @Test
    public void testDeleteChildrenRecursiveDoesNotTouchSiblingsOrParent() throws Exception {
        TestableZooKeeper zk = createClient();
        zk.create("/p", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/p/target", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/p/target/x", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/p/sibling", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/p/sibling/y", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        assertTrue(ZKUtil.deleteChildrenRecursive(zk, "/p/target", 10));

        assertNotNull(zk.exists("/p/target", false));
        assertNull(zk.exists("/p/target/x", false));
        assertNotNull(zk.exists("/p/sibling/y", false));
    }

    @Test(expected = KeeperException.NoNodeException.class)
    public void testDeleteChildrenRecursiveOnMissingNode() throws Exception {
        ZKUtil.deleteChildrenRecursive(createClient(), "/does-not-exist", 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDeleteChildrenRecursiveInvalidPath() throws Exception {
        ZKUtil.deleteChildrenRecursive(createClient(), "no-leading-slash", 10);
    }

    @Test
    public void testListSubTreeBFSPaginatedMatchesDefault() throws Exception {
        TestableZooKeeper zk = createClient();
        zk.create("/t", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        for (int i = 0; i < 20; i++) {
            zk.create("/t/c" + i, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/t/c" + i + "/g", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        List<String> legacy = ZKUtil.listSubTreeBFS(zk, "/t");
        List<String> paginated = ZKUtil.listSubTreeBFS(zk, "/t", true);

        assertEquals(41, legacy.size());
        assertEquals("/t", legacy.get(0));
        assertEquals("/t", paginated.get(0));
        assertEquals(new java.util.HashSet<>(legacy), new java.util.HashSet<>(paginated));
        assertEquals(legacy, ZKUtil.listSubTreeBFS(zk, "/t", false));
    }

    @Test
    public void testDeleteRecursiveStillDeletesRoot() throws Exception {
        TestableZooKeeper zk = createClient();
        zk.create("/r", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/r/c", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        assertTrue(ZKUtil.deleteRecursive(zk, "/r", 10));
        assertNull(zk.exists("/r", false));

        // an empty (leaf) node is also deleted, as before
        zk.create("/leaf", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        assertTrue(ZKUtil.deleteRecursive(zk, "/leaf", 10));
        assertNull(zk.exists("/leaf", false));
    }

    @Test
    public void testListSubTreeBFSPaginatedFromRootBuildsValidPaths() throws Exception {
        TestableZooKeeper zk = createClient();
        zk.create("/rootchild", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/rootchild/g", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        List<String> paginated = ZKUtil.listSubTreeBFS(zk, "/", true);

        assertEquals("/", paginated.get(0));
        assertTrue(paginated.contains("/rootchild"));
        assertTrue(paginated.contains("/rootchild/g"));
        for (String path : paginated) {
            PathUtils.validatePath(path);
        }
    }
}
