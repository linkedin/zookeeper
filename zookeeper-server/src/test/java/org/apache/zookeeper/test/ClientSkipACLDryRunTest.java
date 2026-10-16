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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Runs the client tests with zookeeper.skipACL=dryrun: requests that fail the ACL check
 * are allowed, but the failure is logged.
 */
public class ClientSkipACLDryRunTest extends ClientTest {

    @BeforeClass
    public static void setup() {
        System.setProperty(ZooKeeperServer.SKIP_ACL, ZooKeeperServer.SKIP_ACL_DRYRUN);
    }

    @AfterClass
    public static void teardown() {
        System.clearProperty(ZooKeeperServer.SKIP_ACL);
    }

    @Test
    public void testFailedACLCheckIsLoggedAndAllowed() throws Exception {
        try (LoggerTestTool loggerTestTool = new LoggerTestTool(ZooKeeperServer.class)) {
            ZooKeeper zk = createClient();
            try {
                zk.addAuthInfo("digest", "ben:passwd".getBytes());
                ArrayList<ACL> acls = new ArrayList<>();
                acls.add(new ACL(Perms.ALL, new Id("auth", "")));
                zk.create("/dryruntest", "data".getBytes(), acls, CreateMode.PERSISTENT);
                zk.close();

                zk = createClient();
                zk.addAuthInfo("digest", "ben:wrongpasswd".getBytes());
                try {
                    zk.getData("/dryruntest", false, null);
                    zk.setData("/dryruntest", "newdata".getBytes(), -1);
                } catch (KeeperException e) {
                    throw new AssertionError("Requests failing the ACL check must be allowed in dryrun mode", e);
                }
            } finally {
                zk.close();
            }

            String log = loggerTestTool.getOutputStream().toString();
            assertTrue(log, log.contains("ACL check would have failed"));
            assertTrue(log, log.contains("path=/dryruntest"));
            assertTrue(log, log.contains("requestedPerm=" + Perms.READ));
            assertTrue(log, log.contains("requestedPerm=" + Perms.WRITE));
            assertTrue(log, log.contains("digest:ben"));
            assertFalse(log, log.contains(DigestAuthenticationProvider.generateDigest("ben:passwd")));
            assertFalse(log, log.contains(DigestAuthenticationProvider.generateDigest("ben:wrongpasswd")));
        }
    }

}
