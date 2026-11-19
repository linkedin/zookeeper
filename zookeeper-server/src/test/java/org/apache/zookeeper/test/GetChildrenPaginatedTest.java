/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.jute.BinaryInputArchive;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.PaginationNextPage;
import org.apache.zookeeper.RemoteIterator;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.apache.zookeeper.server.quorum.BufferStats;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class GetChildrenPaginatedTest extends ClientBase {
    private ZooKeeper zk;
    private final Random random = new Random();


    @BeforeEach
    @Override
    public void setUp() throws Exception {
        super.setUp();

        zk = createClient();
    }

    @AfterEach
    @Override
    public void tearDown() throws Exception {
        super.tearDown();

        zk.close();
    }

    @Test
    @Timeout(value = 30)
    public void testPagination() throws Exception {

        final String testId = UUID.randomUUID().toString();
        final String basePath = "/testPagination-" + testId;

        Map<String, Stat> createdChildrenMetadata = createChildren(basePath, 10, 1);

        // Create child 0 out of order (to make sure paths are not ordered lexicographically).
        {
            String childPath = basePath + "/" + 0;
            zk.create(childPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            final Stat stat = zk.exists(childPath, null);

            createdChildrenMetadata.put(String.valueOf(0), stat);

            LOG.info("Created: " + childPath + " czxId: " + stat.getCzxid());
        }

        long minCzxId = -1;
        Set<String> readChildrenMetadata = new HashSet<String>();
        final int pageSize = 3;

        RemoteIterator<String> it = zk.getChildrenIterator(basePath, null, pageSize, minCzxId);

        while (it.hasNext()) {
            final String nodePath = it.next();

            LOG.info("Read: " + nodePath);
            readChildrenMetadata.add(nodePath);

            assertTrue(createdChildrenMetadata.get(nodePath).getCzxid() > minCzxId);
            minCzxId = createdChildrenMetadata.get(nodePath).getCzxid();
        }

        assertEquals(createdChildrenMetadata.keySet(), readChildrenMetadata);
    }

    @Test
    @Timeout(value = 30)
    public void testPaginationIterator() throws Exception {

        final String testId = UUID.randomUUID().toString();
        final String basePath = "/testPagination-" + testId;

        Map<String, Stat> createdChildrenMetadata = createChildren(basePath, random.nextInt(50) + 1, 0);

        Set<String> readChildrenMetadata = new HashSet<String>();

        final int batchSize = random.nextInt(3) + 1;

        RemoteIterator<String> childrenIterator = zk.getChildrenIterator(basePath, null, batchSize, -1);


        while (childrenIterator.hasNext()) {
            String nodePath = childrenIterator.next();

            LOG.info("Read: " + nodePath);
            readChildrenMetadata.add(nodePath);
        }

        assertEquals(createdChildrenMetadata.keySet(), readChildrenMetadata);
    }

    @Test
    @Timeout(value = 30)
    public void testPaginationIteratorOnRoot() throws Exception {
        final String prefix = "/testPaginationRoot-" + UUID.randomUUID();
        List<String> expected = new ArrayList<String>();
        for (int i = 0; i < 10; i++) {
            zk.create(prefix + i, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            expected.add(prefix.substring(1) + i);
        }

        // Collect into a List (not a Set) so duplicated or out-of-order pages are caught.
        List<String> actual = new ArrayList<String>();
        RemoteIterator<String> childrenIterator = zk.getChildrenIterator("/", null, 3, -1);
        while (childrenIterator.hasNext()) {
            actual.add(childrenIterator.next());
        }

        assertEquals(new HashSet<String>(actual).size(), actual.size(), "Root pagination returned duplicates: " + actual);
        assertTrue(actual.containsAll(expected), "Missing children of root: " + expected + " vs " + actual);
        assertTrue(actual.contains("zookeeper"), "Missing /zookeeper: " + actual);

        // The nodes were created in order, so they must come back in that order. Other tests may
        // leave unrelated nodes at the root, so compare only the ones this test created.
        List<String> actualOurs = new ArrayList<String>();
        for (String child : actual) {
            if (child.startsWith(prefix.substring(1))) {
                actualOurs.add(child);
            }
        }
        assertEquals(expected, actualOurs, "Root children returned out of creation order");
    }

    /*
     * This test validates a known list of children is returned by the iterator despite server failures.
     * After the iterator is created successfully, the following logic drives the rest of the test:
     * <ul>
     *     <li>Randomly change the server state (down to up or up to down)</li>
     *     <li>Try to fetch the next element, swallowing exception produced by the server being down</li>
     * </ul>
     * Eventually, all children should be returned regardless of the number of times the server was unavailable.
     */
    @Test
    @Timeout(value = 60)
    public void testPaginationWithServerDown() throws Exception {

        final String testId = UUID.randomUUID().toString();
        final String basePath = "/testPagination-" + testId;

        Map<String, Stat> createdChildrenMetadata = createChildren(basePath, random.nextInt(15) + 10, 0);

        Set<String> readChildrenMetadata = new HashSet<String>();

        final int batchSize = random.nextInt(3) + 1;

        RemoteIterator<String> childrenIterator = zk.getChildrenIterator(basePath, null, batchSize, -1);

        boolean serverDown = false;

        while (childrenIterator.hasNext()) {

            // Randomly change the up/down state of the server
            if (random.nextBoolean()) {
                if (serverDown) {
                    LOG.info("Bringing server UP");
                    startServer();
                    waitForServerUp(hostPort, 5000);
                    serverDown = false;
                } else {
                    LOG.info("Taking server DOWN");
                    stopServer();
                    serverDown = true;
                }
            }

            String child = null;

            boolean exception = false;
            try {
                child = childrenIterator.next();
            } catch (InterruptedException | KeeperException e) {
                LOG.info("Exception in #next(): " + e.getMessage());
                exception = true;
            }

            if (!exception) {
                // next() returned (either more elements in current batch or server is up)
                assertNotNull(child);

                LOG.info("Read: " + child);
                readChildrenMetadata.add(child);
            }
        }

        assertEquals(createdChildrenMetadata.keySet(), readChildrenMetadata);
    }


    class FireOnlyOnceWatcher implements Watcher {
        int watchFiredCount = 0;

        @Override
        public void process(WatchedEvent event) {

            synchronized (this) {
                watchFiredCount += 1;
                this.notify();
            }
        }
    }

    @Test
    @Timeout(value = 30)
    public void testPaginationWatch() throws Exception {

        final String testId = UUID.randomUUID().toString();
        final String basePath = "/testPaginationWatch-" + testId;

        createChildren(basePath, 10, 0);

        long minCzxId = -1;
        final int pageSize = 3;

        FireOnlyOnceWatcher fireOnlyOnceWatcher = new FireOnlyOnceWatcher();

        RemoteIterator<String> it = zk.getChildrenIterator(basePath, fireOnlyOnceWatcher, pageSize, minCzxId);

        int childrenIndex = 0;

        while (it.hasNext()) {

            ++childrenIndex;

            final String nodePath = it.next();
            LOG.info("Read: " + nodePath);

            // Create more children before pagination is completed -- should NOT trigger watch
            if (childrenIndex < 6) {
                String childPath = basePath + "/" + "before-pagination-" + childrenIndex;
                zk.create(childPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

            // Modify the first child of each page.
            // This should not trigger additional watches or create duplicates in the set of children returned
            if (childrenIndex % pageSize == 0) {
                zk.setData(basePath + "/" + nodePath, new byte[3], -1);
            }

            synchronized (fireOnlyOnceWatcher) {
                assertEquals(0, fireOnlyOnceWatcher.watchFiredCount, "Watch should not have fired yet");
            }
        }

        // Create another children after pagination is completed -- should trigger watch
        {
            String childPath = basePath + "/" + "after-pagination";
            zk.create(childPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }


        // Test eventually times out and fails if watches does not fire
        while (true) {
            synchronized (fireOnlyOnceWatcher) {
                if (fireOnlyOnceWatcher.watchFiredCount > 0) {
                    assertEquals(1, fireOnlyOnceWatcher.watchFiredCount, "Watch should have fired once");
                    break;
                }
                fireOnlyOnceWatcher.wait(1000);
            }
        }

        // Watch fired once.

        // Give it another chance to fire (i.e. a duplicate) which would make the test fail
        synchronized (fireOnlyOnceWatcher) {
            fireOnlyOnceWatcher.wait(1000);
            assertEquals(1, fireOnlyOnceWatcher.watchFiredCount, "Watch should have fired once");
        }
    }

    @Test
    @Timeout(value = 60)
    public void testPaginationWithNoChildren() throws Exception {

        final String testId = UUID.randomUUID().toString();
        final String basePath = "/testPagination-" + testId;

        Map<String, Stat> createdChildrenMetadata = createChildren(basePath, 0, 0);

        final int batchSize = 10;

        RemoteIterator<String> childrenIterator = zk.getChildrenIterator(basePath, null, batchSize, -1);

        assertFalse(childrenIterator.hasNext());

        assertThrows(NoSuchElementException.class, childrenIterator::next);
    }

    private Map<String, Stat> createChildren(String basePath, int numChildren, int firstChildrenNameOffset) throws KeeperException, InterruptedException {
        zk.create(basePath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Map<String, Stat> createdChildrenMetadata = new HashMap<String, Stat>();

        for (int i = firstChildrenNameOffset; i < (firstChildrenNameOffset + numChildren); i++) {
            String childPath = basePath + "/" + i;
            zk.create(childPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            final Stat stat = zk.exists(childPath, null);

            createdChildrenMetadata.put(String.valueOf(i), stat);

            LOG.info("Created: " + childPath + " czxid: " + stat.getCzxid());
        }
        return createdChildrenMetadata;
    }

    @Test
    @Timeout(value = 60)
    public void testPaginationWithMulti() throws Exception {

        final String testId = UUID.randomUUID().toString();
        final String basePath = "/testPagination-" + testId;

        final int numChildren = 10;
        final int batchSize = 3;

        zk.create(basePath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Transaction transaction = zk.transaction();
        for (int i = 0; i < numChildren; i++) {
            String childPath = basePath + "/" + i;
            transaction.create(childPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        List<OpResult> transactionOpResults = transaction.commit();

        assertEquals(numChildren, transactionOpResults.size());

        Map<String, Stat> createdChildrenMetadata = new HashMap<>();

        for (int i = 0; i < numChildren; i++) {
            String childPath = basePath + "/" + i;
            final Stat stat = zk.exists(childPath, null);
            createdChildrenMetadata.put(String.valueOf(i), stat);
            LOG.info("Created: " + childPath + " zkId: " + stat.getCzxid());
        }

        Set<String> readChildrenMetadata = new HashSet<String>();

        RemoteIterator<String> childrenIterator = zk.getChildrenIterator(basePath, null, batchSize, -1);

        while (childrenIterator.hasNext()) {

            String children = childrenIterator.next();

            LOG.info("Read: " + children);
            readChildrenMetadata.add(children);
        }

        assertEquals(numChildren, readChildrenMetadata.size());

        assertEquals(createdChildrenMetadata.keySet(), readChildrenMetadata);
    }

    /*
     * Tests if all children can be fetched in one page, the children are
     * in the same order(no sorting by czxid) as the non-paginated result.
     */
    @Test
    @Timeout(value = 60)
    public void testGetAllChildrenPaginatedOnePage() throws KeeperException, InterruptedException {
        final String basePath = "/testPagination-" + UUID.randomUUID().toString();
        createChildren(basePath, 100, 0);

        List<String> expected = zk.getChildren(basePath, false);
        List<String> actual = zk.getAllChildrenPaginated(basePath, false);

        assertEquals(expected, actual);
    }

    /*
     * Tests if all children's packet exceeds jute.maxbuffer, the paginated API can still successfully fetch them.
     */
    @Test
    @Timeout(value = 60)
    public void testGetAllChildrenPaginatedMultiPages() throws InterruptedException, KeeperException {
        // Get the number of children that would definitely exceed 1 MB.
        int numChildren = BinaryInputArchive.maxBuffer / UUID.randomUUID().toString().length() + 1;
        final String basePath = "/testPagination-" + UUID.randomUUID().toString();

        zk.create(basePath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Set<String> expectedChildren = new HashSet<>();

        for (int i = 0; i < numChildren; i += 1000) {
            Transaction transaction = zk.transaction();
            for (int j = i; j < i + 1000 && j < numChildren; j++) {
                String child = UUID.randomUUID().toString();
                String childPath = basePath + "/" + child;
                transaction.create(childPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                expectedChildren.add(child);
            }
            transaction.commit();
        }

        try {
            zk.getChildren(basePath, false);
            fail("Should not succeed to get children because packet length is out of range");
        } catch (KeeperException.ConnectionLossException expected) {
            // ConnectionLossException is expected because packet length exceeds jute.maxbuffer
        }

        // Paginated API can successfully fetch all the children with pagination.
        // If ConnectionLossException is thrown from this method, it possibly means
        // the packet length computing formula in DataTree#getPaginatedChildren needs modification.
        List<String> actualChildren = zk.getAllChildrenPaginated(basePath, false);

        assertEquals(numChildren, actualChildren.size());
        assertEquals(expectedChildren, new HashSet<>(actualChildren));
    }

    /*
     * Tests the packet length formula in DataTree as expected. The packet length calculated
     * is equal to the actual buffer size of the client response.
     */
    @Test
    @Timeout(value = 60)
    public void testPacketLengthFormula() throws KeeperException, InterruptedException {
        final String basePath = "/testPagination-" + UUID.randomUUID().toString();
        zk.create(basePath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Set<String> expectedChildren = new HashSet<>();
        int numChildren = 50 + random.nextInt(50);

        for (int i = 0; i < numChildren; i++) {
            String child = UUID.randomUUID().toString();
            String childPath = basePath + "/" + child;
            zk.create(childPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            expectedChildren.add(child);
        }

        // Fetch max number of children that can fit into one page. The client response stat should record the same
        // buffer size as the packet length calculated by the formula in DataTree.
        PaginationNextPage nextPage = new PaginationNextPage();

        // First page
        List<String> firstPage = zk.getChildren(basePath, null, numChildren, 0, 0, null, nextPage);
        BufferStats clientResponseStats = serverFactory.getZooKeeperServer().serverStats().getClientResponseStats();
        int expectedPacketLength =
                (DataTree.PAGINATION_PACKET_CHILD_EXTRA_BYTES + UUID.randomUUID().toString().length())
                        * firstPage.size() + DataTree.PAGINATION_PACKET_BASE_BYTES;
        int actualBufferSize = clientResponseStats.getLastBufferSize();

        assertEquals(ZooDefs.GetChildrenPaginated.lastPageMinCzxid, nextPage.getMinCzxid(), "The page should be the last page.");
        assertEquals(expectedPacketLength, actualBufferSize, "The client response buffer size should be equal to the packet length calculated by "
                + "pagination formula in DataTree.");
        assertTrue(actualBufferSize <= BinaryInputArchive.maxBuffer);

        assertEquals(expectedChildren, new HashSet<>(firstPage));
    }

    /*
     * Tests below logic:
     * 1. the packet length formula is as expected. The calculated length is equal to the actual
     * client response's buffer size.
     * 2. logic of adding children on server side is correct. It does not miss the last child.
     * 3. watch is only added in the last page.
     */
    @Test
    @Timeout(value = 60)
    public void testMaxNumChildrenPageWatch() throws KeeperException, InterruptedException {
        // Get the max number of UUID children that can return in a page.
        int numChildren = (BinaryInputArchive.maxBuffer - DataTree.PAGINATION_PACKET_BASE_BYTES)
                / (UUID.randomUUID().toString().length() + DataTree.PAGINATION_PACKET_CHILD_EXTRA_BYTES);
        // The packet will exceed 1 MB.
        numChildren++;

        final String basePath = "/testPagination-" + UUID.randomUUID().toString();

        zk.create(basePath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Set<String> expectedChildren = new HashSet<>();

        for (int i = 0; i < numChildren; i += 1000) {
            Transaction transaction = zk.transaction();
            for (int j = i; j < i + 1000 && j < numChildren; j++) {
                String child = UUID.randomUUID().toString();
                String childPath = basePath + "/" + child;
                transaction.create(childPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                expectedChildren.add(child);
            }
            transaction.commit();
        }

        // Fetch max number of children that can fit into one page. The client response stat should record the same
        // buffer size as the packet length calculated by the formula in DataTree.
        PaginationNextPage nextPage = new PaginationNextPage();
        FireOnlyOnceWatcher fireOnlyOnceWatcher = new FireOnlyOnceWatcher();

        // First page
        List<String> firstPage = zk.getChildren(basePath, fireOnlyOnceWatcher, numChildren, 0, 0, null, nextPage);
        BufferStats clientResponseStats = serverFactory.getZooKeeperServer().serverStats().getClientResponseStats();
        int expectedPacketLength =
                (DataTree.PAGINATION_PACKET_CHILD_EXTRA_BYTES + UUID.randomUUID().toString().length())
                        * firstPage.size() + DataTree.PAGINATION_PACKET_BASE_BYTES;
        int actualBufferSize = clientResponseStats.getLastBufferSize();

        assertNotEquals(ZooDefs.GetChildrenPaginated.lastPageMinCzxid, nextPage.getMinCzxid(), "The page should not be the last page.");
        assertEquals(expectedPacketLength, actualBufferSize, "The client response buffer size should be equal to the packet length calculated by "
                + "pagination formula in DataTree.");
        assertTrue(actualBufferSize <= BinaryInputArchive.maxBuffer);

        // Verify the index on server is correct: not adding the last child in the first page.
        assertEquals(numChildren - 1, firstPage.size());

        // Modify the first child of each page.
        // This should not trigger additional watches or create duplicates in the set of children returned
        zk.setData(basePath + "/" + firstPage.get(0), new byte[3], -1);

        synchronized (fireOnlyOnceWatcher) {
            assertEquals(0, fireOnlyOnceWatcher.watchFiredCount, "Watch should not have fired yet");
        }

        // Second page
        List<String> secondPage = zk.getChildren(basePath, fireOnlyOnceWatcher, numChildren, nextPage.getMinCzxid(),
                nextPage.getMinCzxidOffset(), null, nextPage);

        // Verify the logic of adding children to page in DataTree is correct.
        assertEquals(ZooDefs.GetChildrenPaginated.lastPageMinCzxid, nextPage.getMinCzxid());
        assertEquals(1, secondPage.size(), "Should expect only 1 child left in the second page");

        zk.setData(basePath + "/" + secondPage.get(0), new byte[3], -1);

        synchronized (fireOnlyOnceWatcher) {
            assertEquals(0, fireOnlyOnceWatcher.watchFiredCount, "Watch has fired");
        }

        Set<String> actualSet = new HashSet<>(firstPage);
        actualSet.addAll(secondPage);
        assertEquals(expectedChildren, actualSet);
    }

    /*
     * Pages over a stable child list of size 0, 1, page-1, page, page+1 and several pages. Every child is returned
     * exactly once, in creation order, and only the page holding the last child reports the last-page marker.
     */
    @Test
    @Timeout(value = 60)
    public void testStablePageBoundaries() throws Exception {
        final int pageSize = 3;
        for (int numChildren : new int[]{0, 1, pageSize - 1, pageSize, pageSize + 1, 3 * pageSize + 1}) {
            final String basePath = "/testStablePageBoundaries-" + numChildren + "-" + UUID.randomUUID();
            zk.create(basePath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            List<String> created = new ArrayList<>();
            List<Long> czxids = new ArrayList<>();
            for (int i = 0; i < numChildren; i++) {
                // Names sort in the reverse of creation order, so name ordering would be detected.
                String child = "child-" + (numChildren - i);
                zk.create(basePath + "/" + child, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                created.add(child);
                czxids.add(zk.exists(basePath + "/" + child, false).getCzxid());
            }

            List<String> collected = new ArrayList<>();
            PaginationNextPage nextPage = new PaginationNextPage();
            long minCzxid = -1;
            int offset = 0;
            int pages = 0;
            while (true) {
                Stat stat = new Stat();
                List<String> page = zk.getChildren(basePath, null, pageSize, minCzxid, offset, stat, nextPage);
                pages++;
                assertEquals(numChildren, stat.getNumChildren());
                assertTrue(page.size() <= pageSize);
                collected.addAll(page);
                if (nextPage.getMinCzxid() == ZooDefs.GetChildrenPaginated.lastPageMinCzxid) {
                    assertEquals(ZooDefs.GetChildrenPaginated.lastPageCzxidOffset, nextPage.getMinCzxidOffset());
                    break;
                }
                assertEquals(pageSize, page.size(), "Only the last page may be partial");
                assertEquals((long) czxids.get(collected.size()), nextPage.getMinCzxid(),
                        "The next page starts at the first child not yet returned");
                assertEquals(0, nextPage.getMinCzxidOffset());
                minCzxid = nextPage.getMinCzxid();
                offset = nextPage.getMinCzxidOffset();
                assertTrue(pages <= numChildren, "Pagination did not terminate");
            }
            assertEquals(created, collected, "numChildren=" + numChildren);
            assertEquals(Math.max(1, (numChildren + pageSize - 1) / pageSize), pages, "numChildren=" + numChildren);

            List<String> iterated = new ArrayList<>();
            RemoteIterator<String> it = zk.getChildrenIterator(basePath, null, pageSize, -1);
            while (it.hasNext()) {
                iterated.add(it.next());
            }
            assertEquals(created, iterated, "numChildren=" + numChildren);
            assertEquals(new HashSet<>(created), new HashSet<>(zk.getAllChildrenPaginated(basePath, false)));
        }
    }

    /*
     * A child list larger than jute.maxbuffer is returned over several pages, each response staying within the
     * buffer limit, with every child returned exactly once.
     */
    @Test
    @Timeout(value = 120)
    public void testLargeChildSetPagesStayWithinMaxBuffer() throws Exception {
        final int childNameLength = UUID.randomUUID().toString().length();
        final int numChildren = BinaryInputArchive.maxBuffer / childNameLength + 1;
        final String basePath = "/testLargeChildSet-" + UUID.randomUUID();
        zk.create(basePath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Set<String> expectedChildren = new HashSet<>();
        for (int i = 0; i < numChildren; i += 1000) {
            Transaction transaction = zk.transaction();
            for (int j = i; j < i + 1000 && j < numChildren; j++) {
                String child = UUID.randomUUID().toString();
                transaction.create(basePath + "/" + child, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                expectedChildren.add(child);
            }
            transaction.commit();
        }

        BufferStats clientResponseStats = serverFactory.getZooKeeperServer().serverStats().getClientResponseStats();
        clientResponseStats.reset();

        List<String> collected = new ArrayList<>();
        PaginationNextPage nextPage = new PaginationNextPage();
        long minCzxid = 0;
        int offset = 0;
        int pages = 0;
        do {
            List<String> page = zk.getChildren(basePath, null, Integer.MAX_VALUE, minCzxid, offset, null, nextPage);
            pages++;
            assertFalse(page.isEmpty());
            assertTrue(clientResponseStats.getLastBufferSize() <= BinaryInputArchive.maxBuffer);
            collected.addAll(page);
            minCzxid = nextPage.getMinCzxid();
            offset = nextPage.getMinCzxidOffset();
        } while (minCzxid != ZooDefs.GetChildrenPaginated.lastPageMinCzxid && pages <= numChildren);

        assertTrue(pages >= 2, "Expected several pages, got " + pages);
        assertTrue(clientResponseStats.getMaxBufferSize() <= BinaryInputArchive.maxBuffer);
        assertEquals(numChildren, collected.size(), "No child may be returned twice");
        assertEquals(expectedChildren, new HashSet<>(collected));

        Set<String> iterated = new HashSet<>();
        int iteratedCount = 0;
        RemoteIterator<String> it = zk.getChildrenIterator(basePath, null, 5000, -1);
        while (it.hasNext()) {
            iterated.add(it.next());
            iteratedCount++;
        }
        assertEquals(numChildren, iteratedCount);
        assertEquals(expectedChildren, iterated);
    }

    /*
     * The paginated read requires READ permission on the parent, like getChildren. A denied request returns no
     * child names and leaves no watch behind.
     */
    @Test
    @Timeout(value = 60)
    public void testPaginationRequiresReadPermission() throws Exception {
        final String basePath = "/testPaginationAcl-" + UUID.randomUUID();
        final String readerAuth = "reader:password";
        ZooKeeper reader = createClient();
        reader.addAuthInfo("digest", readerAuth.getBytes());
        List<ACL> readerOnly = Collections.singletonList(new ACL(ZooDefs.Perms.READ | ZooDefs.Perms.CREATE
                | ZooDefs.Perms.ADMIN, new Id("digest", DigestAuthenticationProvider.generateDigest(readerAuth))));
        List<ACL> noRead = Collections.singletonList(new ACL(ZooDefs.Perms.ALL & ~ZooDefs.Perms.READ, Ids.ANYONE_ID_UNSAFE));

        zk.create(basePath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create(basePath + "/child-a", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create(basePath + "/child-b", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.setACL(basePath, readerOnly, -1);

        DataTree dataTree = serverFactory.getZooKeeperServer().getZKDatabase().getDataTree();
        final int watchCount = dataTree.getWatchCount();
        FireOnlyOnceWatcher deniedWatcher = new FireOnlyOnceWatcher();
        PaginationNextPage nextPage = new PaginationNextPage();
        Stat stat = new Stat();
        assertThrows(KeeperException.NoAuthException.class,
                () -> zk.getChildren(basePath, deniedWatcher, 10, -1, 0, stat, nextPage));
        assertThrows(KeeperException.NoAuthException.class,
                () -> zk.getChildren(basePath, deniedWatcher, Integer.MAX_VALUE, 0, 0, stat, nextPage));
        assertThrows(KeeperException.NoAuthException.class, () -> zk.getAllChildrenPaginated(basePath, false));
        assertThrows(KeeperException.NoAuthException.class, () -> zk.getChildrenIterator(basePath, deniedWatcher, 1, -1));
        assertThrows(KeeperException.NoAuthException.class, () -> zk.getChildren(basePath, false));
        assertEquals(0, nextPage.getMinCzxid(), "A denied request must not return page information");
        assertEquals(0, stat.getCzxid(), "A denied request must not return the parent stat");
        assertEquals(watchCount, dataTree.getWatchCount(), "A denied request must not leave a watch");

        FireOnlyOnceWatcher allowedWatcher = new FireOnlyOnceWatcher();
        List<String> page = reader.getChildren(basePath, allowedWatcher, 10, -1, 0, null, nextPage);
        assertEquals(Arrays.asList("child-a", "child-b"), page);
        assertEquals(ZooDefs.GetChildrenPaginated.lastPageMinCzxid, nextPage.getMinCzxid());
        assertEquals(watchCount + 1, dataTree.getWatchCount());

        reader.create(basePath + "/child-c", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        synchronized (allowedWatcher) {
            while (allowedWatcher.watchFiredCount == 0) {
                allowedWatcher.wait(1000);
            }
        }
        reader.setACL(basePath, noRead, -1);
        assertThrows(KeeperException.NoAuthException.class,
                () -> reader.getChildren(basePath, null, 10, -1, 0, null, nextPage));
        synchronized (deniedWatcher) {
            assertEquals(0, deniedWatcher.watchFiredCount, "The denied watcher must never fire");
        }

        KeeperException.NoNodeException noNode = assertThrows(KeeperException.NoNodeException.class,
                () -> zk.getChildren(basePath + "/missing", null, 10, -1, 0, null, null));
        assertEquals(basePath + "/missing", noNode.getPath());
        assertThrows(IllegalArgumentException.class, () -> zk.getChildren(basePath, null, 0, -1, 0, null, null));
        assertThrows(IllegalArgumentException.class, () -> zk.getChildren("no-slash", null, 1, -1, 0, null, null));
    }

    /*
     * Paginated reads through a chrooted client address the chrooted parent, including its root, and report
     * client-relative paths in errors and watch events.
     */
    @Test
    @Timeout(value = 60)
    public void testPaginationWithChroot() throws Exception {
        final String chroot = "/testPaginationChroot-" + UUID.randomUUID();
        zk.create(chroot, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        List<String> created = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            String child = "child-" + (7 - i);
            zk.create(chroot + "/" + child, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            created.add(child);
        }

        ZooKeeper chrooted = createClient(hostPort + chroot);
        List<String> collected = new ArrayList<>();
        RemoteIterator<String> it = chrooted.getChildrenIterator("/", null, 2, -1);
        while (it.hasNext()) {
            collected.add(it.next());
        }
        assertEquals(created, collected);
        assertEquals(new HashSet<>(created), new HashSet<>(chrooted.getAllChildrenPaginated("/", false)));

        final BlockingQueue<WatchedEvent> events = new LinkedBlockingQueue<>();
        Watcher childWatcher = event -> {
            if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                events.add(event);
            }
        };
        PaginationNextPage nextPage = new PaginationNextPage();
        Stat stat = new Stat();
        List<String> page = chrooted.getChildren("/", childWatcher, 10, -1, 0, stat, nextPage);
        assertEquals(created, page);
        assertEquals(zk.exists(chroot, false).getCzxid(), stat.getCzxid());
        zk.create(chroot + "/after-watch", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        WatchedEvent event = events.poll(10, TimeUnit.SECONDS);
        assertNotNull(event);
        assertEquals("/", event.getPath());

        KeeperException.NoNodeException noNode = assertThrows(KeeperException.NoNodeException.class,
                () -> chrooted.getChildren("/missing", null, 10, -1, 0, null, null));
        assertEquals("/missing", noNode.getPath());
    }

    /*
     * Documents the continuation contract under concurrent mutation. Pages are ordered by (czxid, name) and the
     * continuation is (next czxid, offset within that czxid). This is not a snapshot: children created before the
     * last page is read are returned, deleted children not yet returned are not, and deleting an already returned
     * child that shares its czxid with later children shifts the offset so a later child can be skipped.
     */
    @Test
    @Timeout(value = 60)
    public void testConcurrentMutationContract() throws Exception {
        final String basePath = "/testPaginationMutation-" + UUID.randomUUID();
        zk.create(basePath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        for (int i = 0; i < 6; i++) {
            zk.create(basePath + "/c" + i, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        PaginationNextPage nextPage = new PaginationNextPage();
        List<String> collected = new ArrayList<>(zk.getChildren(basePath, null, 2, -1, 0, null, nextPage));
        assertEquals(Arrays.asList("c0", "c1"), collected);
        assertEquals(zk.exists(basePath + "/c2", false).getCzxid(), nextPage.getMinCzxid());

        zk.create(basePath + "/n0", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        // Delete the child the continuation points at, and a child that was already returned.
        zk.delete(basePath + "/c2", -1);
        zk.delete(basePath + "/c0", -1);
        collected.addAll(zk.getChildren(basePath, null, 2, nextPage.getMinCzxid(), nextPage.getMinCzxidOffset(), null,
                nextPage));
        collected.addAll(zk.getChildren(basePath, null, 2, nextPage.getMinCzxid(), nextPage.getMinCzxidOffset(), null,
                nextPage));
        assertEquals(ZooDefs.GetChildrenPaginated.lastPageMinCzxid, nextPage.getMinCzxid());
        assertEquals(Arrays.asList("c0", "c1", "c3", "c4", "c5", "n0"), collected);

        // Children created by one multi share a czxid and are ordered by name.
        final String multiPath = "/testPaginationMutationMulti-" + UUID.randomUUID();
        zk.create(multiPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        Transaction transaction = zk.transaction();
        for (int i = 0; i < 5; i++) {
            transaction.create(multiPath + "/m" + i, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        transaction.commit();
        final long multiCzxid = zk.exists(multiPath + "/m0", false).getCzxid();

        List<String> firstPage = zk.getChildren(multiPath, null, 2, -1, 0, null, nextPage);
        assertEquals(Arrays.asList("m0", "m1"), firstPage);
        assertEquals(multiCzxid, nextPage.getMinCzxid());
        assertEquals(2, nextPage.getMinCzxidOffset());

        zk.delete(multiPath + "/m0", -1);
        // Re-reading from offset - 1 no longer starts with the last returned child, which is how
        // getAllChildrenPaginated detects the deletion and restarts the czxid group from offset 0.
        List<String> overlap = zk.getChildren(multiPath, null, 2, multiCzxid, nextPage.getMinCzxidOffset() - 1, null,
                new PaginationNextPage());
        assertNotEquals(firstPage.get(firstPage.size() - 1), overlap.get(0));
        List<String> secondPage = zk.getChildren(multiPath, null, 2, multiCzxid, nextPage.getMinCzxidOffset(), null,
                nextPage);
        assertEquals(Arrays.asList("m3", "m4"), secondPage, "The plain offset continuation skips m2");
        assertEquals(new HashSet<>(Arrays.asList("m1", "m2", "m3", "m4")),
                new HashSet<>(zk.getAllChildrenPaginated(multiPath, false)));

        zk.delete(multiPath + "/m1", -1);
        zk.delete(multiPath + "/m2", -1);
        zk.delete(multiPath + "/m3", -1);
        zk.delete(multiPath + "/m4", -1);
        zk.delete(multiPath, -1);
        assertThrows(KeeperException.NoNodeException.class,
                () -> zk.getChildren(multiPath, null, 2, multiCzxid, 2, null, new PaginationNextPage()));
    }

    /*
     * A child watch left by the last page is an ordinary child watch: it is re-registered after reconnecting and
     * fires once for a later child change.
     */
    @Test
    @Timeout(value = 60)
    public void testPaginatedWatchSurvivesReconnect() throws Exception {
        final String basePath = "/testPaginationReconnect-" + UUID.randomUUID();
        createChildren(basePath, 4, 0);
        final BlockingQueue<WatchedEvent> events = new LinkedBlockingQueue<>();
        Watcher childWatcher = event -> {
            if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                events.add(event);
            }
        };
        PaginationNextPage nextPage = new PaginationNextPage();
        List<String> collected = new ArrayList<>();
        long minCzxid = -1;
        int offset = 0;
        do {
            collected.addAll(zk.getChildren(basePath, childWatcher, 3, minCzxid, offset, null, nextPage));
            minCzxid = nextPage.getMinCzxid();
            offset = nextPage.getMinCzxidOffset();
        } while (minCzxid != ZooDefs.GetChildrenPaginated.lastPageMinCzxid);
        assertEquals(4, collected.size());

        stopServer();
        startServer();
        assertTrue(waitForServerUp(hostPort, CONNECTION_TIMEOUT));
        ZooKeeper other = createClient();
        other.create(basePath + "/after-reconnect", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        WatchedEvent event = events.poll(30, TimeUnit.SECONDS);
        assertNotNull(event, "The watch set by the last page must survive reconnect");
        assertEquals(basePath, event.getPath());
        other.create(basePath + "/second", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        assertNull(events.poll(1, TimeUnit.SECONDS), "A child watch fires only once");
    }
}
