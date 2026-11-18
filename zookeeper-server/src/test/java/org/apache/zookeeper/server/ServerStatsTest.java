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


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ServerStatsTest extends ZKTestCase {

    private ServerStats.Provider providerMock;

    @BeforeEach
    public void setUp() {
        providerMock = mock(ServerStats.Provider.class);
    }

    @Test
    public void testPacketsMetrics() {
        // Given ...
        ServerStats serverStats = new ServerStats(providerMock);
        int incrementCount = 20;

        // When increment ...
        for (int i = 0; i < incrementCount; i++) {
            serverStats.incrementPacketsSent();
            serverStats.incrementPacketsReceived();
            serverStats.incrementPacketsReceived();
        }

        // Then ...
        assertEquals(incrementCount, serverStats.getPacketsSent());
        assertEquals(incrementCount * 2, serverStats.getPacketsReceived());

        // When reset ...
        serverStats.resetRequestCounters();

        // Then ...
        assertAllPacketsZero(serverStats);

    }

    @Test
    public void testLatencyMetrics() {
        // Given ...
        ServerStats serverStats = new ServerStats(providerMock);

        // When incremented...
        Request fakeRequest = new Request(0, 0, 0, null, null, 0);
        serverStats.updateLatency(fakeRequest, fakeRequest.createTime + 1000);
        serverStats.updateLatency(fakeRequest, fakeRequest.createTime + 2000);

        // Then ...
        assertThat("Max latency check", 2000L, lessThanOrEqualTo(serverStats.getMaxLatency()));
        assertThat("Min latency check", 1000L, lessThanOrEqualTo(serverStats.getMinLatency()));
        assertEquals(1500, serverStats.getAvgLatency(), 200);

        // When reset...
        serverStats.resetLatency();

        // Then ...
        assertAllLatencyZero(serverStats);
    }

    @Test
    public void testFsyncThresholdExceedMetrics() {
        // Given ...
        ServerStats serverStats = new ServerStats(providerMock);
        int incrementCount = 30;

        // When increment ...
        for (int i = 0; i < incrementCount; i++) {
            serverStats.incrementFsyncThresholdExceedCount();
        }

        // Then ...
        assertEquals(incrementCount, serverStats.getFsyncThresholdExceedCount());

        // When reset ...
        serverStats.resetFsyncThresholdExceedCount();

        // Then ...
        assertFsyncThresholdExceedCountZero(serverStats);

    }

    @Test
    public void testReset() {
        // Given ...
        ServerStats serverStats = new ServerStats(providerMock);

        assertAllPacketsZero(serverStats);
        assertAllLatencyZero(serverStats);

        // When ...
        Request fakeRequest = new Request(0, 0, 0, null, null, 0);
        serverStats.incrementPacketsSent();
        serverStats.incrementPacketsReceived();
        serverStats.updateLatency(fakeRequest, fakeRequest.createTime + 1000);

        serverStats.reset();

        // Then ...
        assertAllPacketsZero(serverStats);
        assertAllLatencyZero(serverStats);
    }

    @Test
    public void testReadWriteRequestStatsAndResets() {
        ServerStats serverStats = new ServerStats(providerMock);
        Request request = new Request(0, 0, 0, null, null, 0);
        long start = request.createTime;

        serverStats.updateLatency(OpCode.getData, request, start + 10);
        serverStats.updateLatency(OpCode.exists, request, start + 30);
        serverStats.updateLatency(OpCode.setData, request, start + 50);
        // Operations outside the read and write sets only feed the overall latency.
        serverStats.updateLatency(OpCode.ping, request, start + 1000);
        serverStats.updateLatency(OpCode.check, request, start + 1000);
        serverStats.updateLatency(OpCode.create2, request, start + 1000);
        serverStats.updateLatency(OpCode.getData, request, start - 1);

        assertEquals(1000L, serverStats.getMaxLatency());
        assertReadStats(serverStats, 2, 10, 20, 30);
        assertWriteStats(serverStats, 1, 50, 50, 50);

        serverStats.resetMaxLatency();
        assertReadStats(serverStats, 2, 10, 20, 10);
        assertWriteStats(serverStats, 1, 50, 50, 50);

        serverStats.resetLatency();
        assertReadStats(serverStats, 2, 0, 0, 0);
        assertWriteStats(serverStats, 1, 0, 0, 0);

        serverStats.updateLatency(OpCode.getData, request, start + 40);
        assertReadStats(serverStats, 3, 40, 40, 40);

        serverStats.resetRequestCounters();
        assertReadStats(serverStats, 0, 40, 40, 40);
        assertWriteStats(serverStats, 0, 0, 0, 0);

        serverStats.incrementConnectionCreated();
        serverStats.incrementConnectionClosed();
        serverStats.incrementSessionCreated();
        serverStats.incrementSessionClosed();
        assertConnectionCounters(serverStats, 1);

        serverStats.reset();
        assertReadStats(serverStats, 0, 0, 0, 0);
        assertConnectionCounters(serverStats, 0);
    }

    private void assertReadStats(ServerStats serverStats, long count, long min, long avg, long max) {
        assertEquals(count, serverStats.getReadCount());
        assertEquals(min, serverStats.getMinReadLatency());
        assertEquals(avg, serverStats.getAvgReadLatency());
        assertEquals(max, serverStats.getMaxReadLatency());
    }

    private void assertWriteStats(ServerStats serverStats, long count, long min, long avg, long max) {
        assertEquals(count, serverStats.getWriteCount());
        assertEquals(min, serverStats.getMinWriteLatency());
        assertEquals(avg, serverStats.getAvgWriteLatency());
        assertEquals(max, serverStats.getMaxWriteLatency());
    }

    private void assertConnectionCounters(ServerStats serverStats, long expected) {
        assertEquals(expected, serverStats.getConnectionCreateCount());
        assertEquals(expected, serverStats.getConnectionCloseCount());
        assertEquals(expected, serverStats.getSessionCreateCount());
        assertEquals(expected, serverStats.getSessionCloseCount());
    }

    private void assertAllPacketsZero(ServerStats serverStats) {
        assertEquals(0L, serverStats.getPacketsSent());
        assertEquals(0L, serverStats.getPacketsReceived());
    }

    private void assertAllLatencyZero(ServerStats serverStats) {
        assertEquals(0L, serverStats.getMaxLatency());
        assertEquals(0L, serverStats.getMinLatency());
        assertEquals(0, serverStats.getAvgLatency(), 0.00001);
    }

    private void assertFsyncThresholdExceedCountZero(ServerStats serverStats) {
        assertEquals(0L, serverStats.getFsyncThresholdExceedCount());
    }

}
