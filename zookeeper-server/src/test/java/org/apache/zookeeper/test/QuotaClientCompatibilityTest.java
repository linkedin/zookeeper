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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;
import java.io.File;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.StatsTrack;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.cli.SetQuotaCommand;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class QuotaClientCompatibilityTest extends ClientBase {

    private static final String BASELINE_CLASSPATH = "zookeeper.test.baselineClientClasspath";

    @BeforeClass
    public static void configureLimits() {
        System.setProperty("zookeeper.enforceQuota", "true");
        System.setProperty("zookeeper.ephemeralNodes.total.byte.limit", "0");
    }

    @AfterClass
    public static void clearLimitProperties() {
        System.clearProperty("zookeeper.enforceQuota");
        System.clearProperty("zookeeper.ephemeralNodes.total.byte.limit");
    }

    @Test
    public void testCurrentClientReceivesDistinctWireErrors() throws Exception {
        createLimitedPath();
        String classpath = System.getProperty("java.class.path");
        assertProbe(classpath, hostPort, "/limited/child", CreateMode.PERSISTENT,
            "KEEPER\t-125\tQuotaExceededException", 1);
        assertProbe(classpath, hostPort, "/ephemeral", CreateMode.EPHEMERAL,
            "KEEPER\t-128\tTotalEphemeralLimitExceeded", 1);
    }

    @Test
    public void testMixedVersionWireErrors() throws Exception {
        String baseline = System.getProperty(BASELINE_CLASSPATH);
        assumeNotNull(baseline);
        String testClasses = Paths.get(QuotaClientCompatibilityProbe.class.getProtectionDomain()
            .getCodeSource().getLocation().toURI()).toString();
        String oldClasspath = testClasses + File.pathSeparator + baseline;
        createLimitedPath();
        assertProbe(oldClasspath, hostPort, "/limited/child", CreateMode.PERSISTENT,
            "KEEPER\t-125\tTotalEphemeralLimitExceeded", 1);
        assertProbe(oldClasspath, hostPort, "/ephemeral", CreateMode.EPHEMERAL,
            "RUNTIME\tNullPointerException", 2);

        File directory = createTmpDir();
        File config = new File(directory, "zoo.cfg");
        File log = new File(directory, "server.log");
        int port = PortAssignment.unique();
        Properties properties = new Properties();
        properties.setProperty("clientPort", Integer.toString(port));
        properties.setProperty("clientPortAddress", "127.0.0.1");
        properties.setProperty("dataDir", directory.getAbsolutePath());
        properties.setProperty("tickTime", "2000");
        try (OutputStream output = Files.newOutputStream(config.toPath())) {
            properties.store(output, null);
        }
        Process oldServer = new ProcessBuilder(javaExecutable(),
            "-Dzookeeper.ephemeralNodes.total.byte.limit=0",
            "-Dzookeeper.admin.enableServer=false",
            "-Dzookeeper.4lw.commands.whitelist=*",
            "-cp", oldClasspath, "org.apache.zookeeper.server.ZooKeeperServerMain", config.getAbsolutePath())
            .redirectErrorStream(true).redirectOutput(log).start();
        try {
            String address = "127.0.0.1:" + port;
            assertTrue("Baseline server did not start; see " + log, waitForServerUp(address, CONNECTION_TIMEOUT));
            assertProbe(System.getProperty("java.class.path"), address, "/new-client", CreateMode.EPHEMERAL,
                "KEEPER\t-125\tQuotaExceededException", 1);
            assertProbe(oldClasspath, address, "/old-client", CreateMode.EPHEMERAL,
                "KEEPER\t-125\tTotalEphemeralLimitExceeded", 1);
        } finally {
            stopProcess(oldServer);
        }
    }

    private void createLimitedPath() throws Exception {
        ZooKeeper zk = createClient();
        zk.create("/limited", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        StatsTrack limits = new StatsTrack();
        limits.setCountHardLimit(1);
        SetQuotaCommand.createQuota(zk, "/limited", limits);
        zk.close();
    }

    private void assertProbe(String classpath, String address, String path, CreateMode mode,
                             String expectedOutput, int expectedExit) throws Exception {
        File log = File.createTempFile("quota-client-", ".log", testBaseDir);
        Process process = new ProcessBuilder(javaExecutable(), "-Dzookeeper.sasl.client=false",
            "-cp", classpath, QuotaClientCompatibilityProbe.class.getName(), address, path, mode.name())
            .redirectErrorStream(true).redirectOutput(log).start();
        try {
            assertTrue("Client probe timed out; see " + log,
                process.waitFor(CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS));
            String output = new String(Files.readAllBytes(log.toPath()), UTF_8);
            assertEquals(output, expectedExit, process.exitValue());
            assertTrue(output, output.contains("QUOTA_PROBE\t" + expectedOutput));
        } finally {
            stopProcess(process);
        }
    }

    private static String javaExecutable() {
        return new File(System.getProperty("java.home"), "bin/java").getPath();
    }

    private static void stopProcess(Process process) throws InterruptedException {
        process.destroy();
        if (!process.waitFor(CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)) {
            process.destroyForcibly();
            assertTrue("Child process did not stop", process.waitFor(CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS));
        }
    }
}
