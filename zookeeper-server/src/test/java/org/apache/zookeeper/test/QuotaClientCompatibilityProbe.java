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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * Runs with either client version in a separate JVM, using only the shared 3.6 API.
 */
public final class QuotaClientCompatibilityProbe {

    private QuotaClientCompatibilityProbe() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException("Expected connect string, path, and create mode");
        }
        CountDownLatch connected = new CountDownLatch(1);
        ZooKeeper zk = new ZooKeeper(args[0], 30000, event -> {
            if (event.getState() == KeeperState.SyncConnected) {
                connected.countDown();
            }
        });
        int result = 0;
        try {
            if (!connected.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Client did not connect");
            }
            zk.create(args[1], new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.valueOf(args[2]));
            System.out.println("QUOTA_PROBE\tOK");
        } catch (KeeperException error) {
            System.out.println("QUOTA_PROBE\tKEEPER\t" + error.code().intValue()
                + "\t" + error.getClass().getSimpleName());
            result = 1;
        } catch (RuntimeException error) {
            System.out.println("QUOTA_PROBE\tRUNTIME\t" + error.getClass().getSimpleName());
            error.printStackTrace(System.err);
            result = 2;
        } finally {
            zk.close();
        }
        System.exit(result);
    }
}
