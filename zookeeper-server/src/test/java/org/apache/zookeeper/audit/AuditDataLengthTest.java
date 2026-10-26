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

package org.apache.zookeeper.audit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.test.ClientBase;
import org.apache.zookeeper.test.LoggerTestTool;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class AuditDataLengthTest extends ClientBase {
    private static ByteArrayOutputStream os;

    @BeforeClass
    public static void setup() {
        System.setProperty(ZKAuditProvider.AUDIT_ENABLE, "true");
        System.setProperty(ZKAuditProvider.AUDIT_DATA_LENGTH_ENABLE, "true");
        LoggerTestTool loggerTestTool = new LoggerTestTool(Slf4jAuditLogger.class);
        os = loggerTestTool.getOutputStream();
    }

    @AfterClass
    public static void teardown() {
        System.clearProperty(ZKAuditProvider.AUDIT_ENABLE);
        System.clearProperty(ZKAuditProvider.AUDIT_DATA_LENGTH_ENABLE);
    }

    @Test
    public void testDataLengthIsLoggedForCreateAndSetData() throws Exception {
        final ZooKeeper zk = createClient();
        readAuditLog(os);
        zk.create("/empty", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        assertLog("operation=create\tznode=/empty\tznode_type=persistent\tdata_length=0\tresult=success");

        zk.create("/node", new byte[100], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        assertLog("operation=create\tznode=/node\tznode_type=persistent\tdata_length=100\tresult=success");

        zk.setData("/node", new byte[2048], -1);
        assertLog("operation=setData\tznode=/node\tdata_length=2048\tresult=success");

        zk.delete("/node", -1);
        assertLog("operation=delete\tznode=/node\tresult=success");
    }

    @Test
    public void testDataLengthIsLoggedForFailedWrites() throws Exception {
        final ZooKeeper zk = createClient();
        zk.create("/exists", new byte[1], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        readAuditLog(os);
        try {
            zk.create("/exists", new byte[10], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            fail("Expected NodeExistsException");
        } catch (KeeperException.NodeExistsException e) {
            // expected
        }
        assertLog("operation=create\tznode=/exists\tznode_type=persistent\tdata_length=10\tresult=failure");

        try {
            zk.setData("/missing", new byte[20], -1);
            fail("Expected NoNodeException");
        } catch (KeeperException.NoNodeException e) {
            // expected
        }
        assertLog("operation=setData\tznode=/missing\tdata_length=20\tresult=failure");
    }

    @Test
    public void testDataLengthIsLoggedPerMultiOperation() throws Exception {
        final ZooKeeper zk = createClient();
        zk.create("/multi", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        readAuditLog(os);
        zk.multi(Arrays.asList(
                Op.check("/multi", -1),
                Op.create("/multi/a", new byte[5], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
                Op.setData("/multi", new byte[7], -1),
                Op.create("/multi/b", new byte[3], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
                Op.delete("/multi/b", -1)));
        List<String> logs = readAuditLog(os);
        assertEquals(4, logs.size());
        assertTrue(logs.get(0), logs.get(0).endsWith("operation=create\tznode=/multi/a\tznode_type=persistent\tdata_length=5\tresult=success"));
        assertTrue(logs.get(1), logs.get(1).endsWith("operation=setData\tznode=/multi\tdata_length=7\tresult=success"));
        assertTrue(logs.get(2), logs.get(2).endsWith("operation=create\tznode=/multi/b\tznode_type=persistent\tdata_length=3\tresult=success"));
        assertTrue(logs.get(3), logs.get(3).endsWith("operation=delete\tznode=/multi/b\tresult=success"));
    }

    private static void assertLog(String expectedSuffix) throws IOException {
        List<String> logs = readAuditLog(os);
        assertEquals(logs.toString(), 1, logs.size());
        assertTrue(logs.get(0), logs.get(0).endsWith(expectedSuffix));
    }

    private static List<String> readAuditLog(ByteArrayOutputStream os) throws IOException {
        List<String> logs = new ArrayList<>();
        LineNumberReader r = new LineNumberReader(new StringReader(os.toString()));
        String line;
        while ((line = r.readLine()) != null) {
            logs.add(line);
        }
        os.reset();
        return logs;
    }
}
