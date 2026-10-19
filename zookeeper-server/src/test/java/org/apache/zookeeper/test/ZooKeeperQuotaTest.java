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

package org.apache.zookeeper.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Quotas;
import org.apache.zookeeper.StatsTrack;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.cli.CliCommand;
import org.apache.zookeeper.cli.DelQuotaCommand;
import org.apache.zookeeper.cli.ListQuotaCommand;
import org.apache.zookeeper.cli.MalformedPathException;
import org.apache.zookeeper.cli.SetQuotaCommand;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.Test;

public class ZooKeeperQuotaTest extends ClientBase {

    @Test
    public void testQuota() throws Exception {
        final ZooKeeper zk = createClient();
        final String path = "/a/b/v";
        // making sure setdata works on /
        zk.setData("/", "some".getBytes(), -1);
        zk.create("/a", "some".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        zk.create("/a/b", "some".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        zk.create("/a/b/v", "some".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        zk.create("/a/b/v/d", "some".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        SetQuotaCommand.createQuota(zk, path,
            new StatsTrack("count=10,bytes=5=;byteHardLimit=16;countHardLimit=20"));

        // see if its set
        String absolutePath = Quotas.limitPath(path);
        byte[] data = zk.getData(absolutePath, false, new Stat());
        StatsTrack st = new StatsTrack(data);
        assertTrue("bytes are set", st.getBytes() == 5L);
        assertTrue("num count is set", st.getCount() == 10);
        assertEquals(16L, st.getByteHardLimit());
        assertEquals(20L, st.getCountHardLimit());

        String statPath = Quotas.statPath(path);
        byte[] qdata = zk.getData(statPath, false, new Stat());
        StatsTrack qst = new StatsTrack(qdata);
        assertTrue("bytes are set", qst.getBytes() == 8L);
        assertTrue("count is set", qst.getCount() == 2);

        //force server to restart and load from snapshot, not txn log
        stopServer();
        startServer();
        stopServer();
        startServer();
        ZooKeeperServer server = serverFactory.getZooKeeperServer();
        assertEquals("Quota is still set", path,
            server.getZKDatabase().getDataTree().getMaxPrefixWithQuota(path + "/d"));
        List<StatsTrack> recovered = ListQuotaCommand.listQuota(createClient(), path);
        assertEquals(st.toString(), recovered.get(0).toString());
        assertEquals(qst.toString(), recovered.get(1).toString());
    }

    @Test
    public void testSetQuota() throws IOException, InterruptedException, KeeperException, MalformedPathException {
        final ZooKeeper zk = createClient();

        String path = "/c1";
        String nodeData = "foo";
        zk.create(path, nodeData.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        int count = 10;
        long bytes = 5L;
        StatsTrack quota = new StatsTrack();
        quota.setBytes(bytes);
        quota.setCount(count);
        SetQuotaCommand.createQuota(zk, path, quota);

        //check the limit
        String absoluteLimitPath = Quotas.limitPath(path);
        byte[] data = zk.getData(absoluteLimitPath, false, null);
        StatsTrack st = new StatsTrack(data);
        assertEquals(bytes, st.getBytes());
        assertEquals(count, st.getCount());
        //check the stats
        String absoluteStatPath = Quotas.statPath(path);
        data = zk.getData(absoluteStatPath, false, null);
        st = new StatsTrack(data);
        assertEquals(nodeData.length(), st.getBytes());
        assertEquals(1, st.getCount());

        //create another node
        String path2 = "/c1/c2";
        String nodeData2 = "bar";
        zk.create(path2, nodeData2.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        absoluteStatPath = Quotas.statPath(path);
        data = zk.getData(absoluteStatPath, false, null);
        st = new StatsTrack(data);
        //check the stats
        assertEquals(nodeData.length() + nodeData2.length(), st.getBytes());
        assertEquals(2, st.getCount());
    }

    @Test
    public void testSetQuotaWhenSetQuotaOnParentOrChildPath() throws IOException, InterruptedException, KeeperException, MalformedPathException {
        final ZooKeeper zk = createClient();

        zk.create("/c1", "some".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/c1/c2", "some".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/c1/c2/c3", "some".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/c1/c2/c3/c4", "some".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/c1/c2/c3/c4/c5", "some".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        //set the quota on the path:/c1/c2/c3
        StatsTrack quota = new StatsTrack("count=10,bytes=5=;byteHardLimit=50;countHardLimit=100");
        SetQuotaCommand.createQuota(zk, "/c1/c2/c3", quota);

        try {
            SetQuotaCommand.createQuota(zk, "/c1", quota);
            fail("A quota must not overlap a child quota");
        } catch (IllegalArgumentException e) {
            assertEquals("/c1 has a child /c1/c2/c3 which has a quota", e.getMessage());
        }

        try {
            SetQuotaCommand.createQuota(zk, "/c1/c2/c3/c4/c5", quota);
            fail("A quota must not overlap a parent quota");
        } catch (IllegalArgumentException e) {
            assertEquals("/c1/c2/c3/c4/c5 has a parent /c1/c2/c3 which has a quota", e.getMessage());
        }
    }

    @Test
    public void testQuotaUpdatesPreserveOtherFields() throws Exception {
        ZooKeeper zk = createClient();
        String path = "/quotaFields";
        zk.create(path, "foo".getBytes(StandardCharsets.UTF_8), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        SetQuotaCommand.createQuota(zk, path, new StatsTrack("count=10,bytes=20"));
        String limitPath = Quotas.limitPath(path);
        zk.setData(limitPath,
            "count=10,bytes=20=;byteHardLimit=40;countHardLimit=30".getBytes(StandardCharsets.UTF_8), -1);

        String[][] updates = {
            {"-n", "12", "count=12,bytes=20=;byteHardLimit=40;countHardLimit=30"},
            {"-b", "22", "count=12,bytes=22=;byteHardLimit=40;countHardLimit=30"},
            {"-N", "32", "count=12,bytes=22=;byteHardLimit=40;countHardLimit=32"},
            {"-B", "42", "count=12,bytes=22=;byteHardLimit=42;countHardLimit=32"}
        };
        for (String[] update : updates) {
            execQuotaCommand(new SetQuotaCommand(), zk, "setquota", update[0], update[1], path);
            assertEquals(update[0], update[2],
                new String(zk.getData(limitPath, false, null), StandardCharsets.UTF_8));
        }

        String output = execQuotaCommand(new ListQuotaCommand(), zk, "listquota", path);
        assertEquals("Output quota for " + path + " count=12,bytes=22=;byteHardLimit=42;countHardLimit=32"
            + System.lineSeparator() + "Output stat for " + path + " count=1,bytes=3" + System.lineSeparator(),
            output);
    }

    @Test
    public void testQuotaDeletesPreserveOtherFields() throws Exception {
        ZooKeeper zk = createClient();
        String path = "/quotaDeletes";
        zk.create(path, "foo".getBytes(StandardCharsets.UTF_8), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        SetQuotaCommand.createQuota(zk, path, new StatsTrack("count=10,bytes=20"));
        String limitPath = Quotas.limitPath(path);
        zk.setData(limitPath,
            "count=10,bytes=20=;byteHardLimit=40;countHardLimit=30".getBytes(StandardCharsets.UTF_8), -1);

        String[][] deletes = {
            {"-n", "count=-1,bytes=20=;byteHardLimit=40;countHardLimit=30"},
            {"-b", "count=-1,bytes=-1=;byteHardLimit=40;countHardLimit=30"},
            {"-N", "count=-1,bytes=-1=;byteHardLimit=40;countHardLimit=-1"},
            {"-B", "count=-1,bytes=-1=;byteHardLimit=-1;countHardLimit=-1"}
        };
        for (String[] delete : deletes) {
            execQuotaCommand(new DelQuotaCommand(), zk, "delquota", delete[0], path);
            assertEquals(delete[0], delete[1],
                new String(zk.getData(limitPath, false, null), StandardCharsets.UTF_8));
            assertEquals("count=1,bytes=3",
                new String(zk.getData(Quotas.statPath(path), false, null), StandardCharsets.UTF_8));
        }

        execQuotaCommand(new DelQuotaCommand(), zk, "delquota", path);
        assertNull(zk.exists(limitPath, false));
        assertNull(zk.exists(Quotas.statPath(path), false));
        assertNull(zk.exists(Quotas.quotaPath(path), false));
        assertNotNull(zk.exists(Quotas.quotaZookeeper, false));
        assertNull(serverFactory.getZooKeeperServer().getZKDatabase().getDataTree().getMaxPrefixWithQuota(path));
    }

    @Test
    public void testHardQuotaCliBoundaries() throws Exception {
        ZooKeeper zk = createClient();
        String path = "/hardQuota";
        zk.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        execQuotaCommand(new SetQuotaCommand(), zk, "setquota", "-N", "2147483647", path);
        execQuotaCommand(new SetQuotaCommand(), zk, "setquota", "-B", "0", path);
        String limitPath = Quotas.limitPath(path);
        assertEquals("count=-1,bytes=-1=;byteHardLimit=0;countHardLimit=2147483647",
            new String(zk.getData(limitPath, false, null), StandardCharsets.UTF_8));

        execQuotaCommand(new SetQuotaCommand(), zk, "setquota", "-B", "9223372036854775807", path);
        assertEquals("count=-1,bytes=-1=;byteHardLimit=9223372036854775807;countHardLimit=2147483647",
            new String(zk.getData(limitPath, false, null), StandardCharsets.UTF_8));
    }

    @Test
    public void testInvalidQuotaCliValuesDoNotWriteLimits() throws Exception {
        ZooKeeper zk = createClient();
        String path = "/invalidQuota";
        zk.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        String[][] invalidValues = {
            {"-n", "0", "greater than zero"},
            {"-n", "-1", "greater than zero"},
            {"-N", "0", "greater than zero"},
            {"-N", "-1", "greater than zero"},
            {"-b", "-1", "greater than or equal to zero"},
            {"-B", "-1", "greater than or equal to zero"},
            {"-n", "2147483648", "NumberFormatException"},
            {"-N", "2147483648", "NumberFormatException"},
            {"-b", "9223372036854775808", "NumberFormatException"},
            {"-B", "9223372036854775808", "NumberFormatException"},
            {"-n", "not-a-number", "NumberFormatException"},
            {"-B", "not-a-number", "NumberFormatException"}
        };
        for (String[] invalid : invalidValues) {
            SetQuotaCommand command = new SetQuotaCommand();
            ByteArrayOutputStream errors = new ByteArrayOutputStream();
            command.setErr(new PrintStream(errors, true, StandardCharsets.UTF_8.name()));
            command.setZk(zk);
            command.parse(new String[]{"setquota", invalid[0], invalid[1], path});

            assertFalse(command.exec());
            assertTrue(invalid[0] + " " + invalid[1],
                errors.toString(StandardCharsets.UTF_8.name()).contains(invalid[2]));
            assertNull(zk.exists(Quotas.limitPath(path), false));
        }
    }

    @Test
    public void testQuotaCliPathRestrictions() throws Exception {
        ZooKeeper zk = createClient();
        SetQuotaCommand command = new SetQuotaCommand();
        ByteArrayOutputStream errors = new ByteArrayOutputStream();
        command.setErr(new PrintStream(errors, true, StandardCharsets.UTF_8.name()));
        command.setZk(zk);
        command.parse(new String[]{"setquota", "-n", "1", Quotas.quotaZookeeper});
        assertFalse(command.exec());
        assertTrue(errors.toString(StandardCharsets.UTF_8.name()).contains("cannot set a quota under the path"));

        for (String path : new String[]{"not-absolute", "/does-not-exist"}) {
            try {
                execQuotaCommand(new SetQuotaCommand(), zk, "setquota", "-n", "1", path);
                fail("Expected a malformed or missing quota target to be rejected");
            } catch (MalformedPathException expected) {
                assertNotNull(expected.getMessage());
            }
        }
    }

    @Test
    public void testStaticQuotaHelpersPreserveUnselectedFields() throws Exception {
        ZooKeeper zk = createClient();
        String path = "/staticQuota";
        zk.create(path, new byte[2], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        StatsTrack quota = new StatsTrack("count=10,bytes=20=;byteHardLimit=40;countHardLimit=30;futureLimit=9");
        SetQuotaCommand.createQuota(zk, path, quota);

        StatsTrack update = new StatsTrack();
        update.setBytes(22);
        SetQuotaCommand.createQuota(zk, path, update);
        StatsTrack delete = new StatsTrack();
        delete.setCount(1);
        delete.setByteHardLimit(1);
        DelQuotaCommand.delQuota(zk, path, delete);

        List<StatsTrack> listed = ListQuotaCommand.listQuota(zk, path);
        assertEquals(2, listed.size());
        assertEquals("count=-1,bytes=22=;byteHardLimit=-1;countHardLimit=30;futureLimit=9", listed.get(0).toString());
        assertEquals("count=1,bytes=2", listed.get(1).toString());
    }

    private static String execQuotaCommand(CliCommand command, ZooKeeper zk, String... args) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        command.setOut(new PrintStream(output, true, StandardCharsets.UTF_8.name()));
        command.setZk(zk);
        command.parse(args);
        assertFalse(command.exec());
        return output.toString(StandardCharsets.UTF_8.name());
    }

}
