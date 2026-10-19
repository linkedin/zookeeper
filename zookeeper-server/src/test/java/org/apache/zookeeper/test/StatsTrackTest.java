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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import java.nio.charset.StandardCharsets;
import org.apache.zookeeper.StatsTrack;
import org.apache.zookeeper.ZKTestCase;
import org.junit.Test;

public class StatsTrackTest extends ZKTestCase {

    @Test
    public void testHardLimitsRoundTripThroughLegacyParser() {
        String wire = "count=2,bytes=4=;byteHardLimit=8;countHardLimit=3";
        StatsTrack stats = new StatsTrack();
        stats.setCount(2);
        stats.setBytes(4);
        stats.setCountHardLimit(3);
        stats.setByteHardLimit(8);
        String serialized = stats.toString();

        // This is the two-field parser used before hard quotas were introduced.
        String[] fields = serialized.split(",");
        assertEquals(2, fields.length);
        assertEquals(2, Integer.parseInt(fields[0].split("=")[1]));
        assertEquals(4L, Long.parseLong(fields[1].split("=")[1]));
        assertEquals(wire, serialized);
        StatsTrack restored = new StatsTrack(stats.getStatsBytes());
        assertEquals(3L, restored.getCountHardLimit());
        assertEquals(8L, restored.getByteHardLimit());
        assertEquals(stats, restored);
    }

    @Test
    public void testLongCountsAndUnknownFieldsRoundTrip() {
        StatsTrack stats = new StatsTrack(
            "count=2147483648,bytes=9223372036854775807=;futureLimit=9;countHardLimit=4294967296");

        assertEquals(2147483648L, stats.getCount());
        assertEquals(Long.MAX_VALUE, stats.getBytes());
        assertEquals(4294967296L, stats.getCountHardLimit());
        assertEquals("count=2147483648,bytes=9223372036854775807=;countHardLimit=4294967296;futureLimit=9",
            stats.toString());
        stats.setCount(4294967296L);
        assertEquals(4294967296L, new StatsTrack(stats.getStatsBytes()).getCount());
    }

    @Test
    public void testEmptyAndMissingValuesAreUnset() {
        assertEquals("count=-1,bytes=-1", new StatsTrack().toString());
        assertEquals("count=-1,bytes=-1", new StatsTrack((String) null).toString());
        assertEquals("count=-1,bytes=-1", new StatsTrack("").toString());
        assertEquals("count=-1,bytes=4=;countHardLimit=3",
            new StatsTrack("bytes=4;countHardLimit=3").toString());
        assertEquals("count=-1,bytes=4=;countHardLimit=3",
            new StatsTrack("count==ignored;bytes=4;countHardLimit=3").toString());
    }

    @Test
    public void testEqualityIncludesHardLimitsAndUnknownFields() {
        StatsTrack stats = new StatsTrack("count=2,bytes=4=;countHardLimit=3;futureLimit=9");
        StatsTrack reordered = new StatsTrack("count=2,bytes=4=;futureLimit=9;countHardLimit=3");

        assertEquals(stats, reordered);
        assertEquals(stats.hashCode(), reordered.hashCode());
        assertNotEquals(stats, new StatsTrack("count=2,bytes=4=;countHardLimit=5;futureLimit=9"));
        assertNotEquals(stats, new StatsTrack("count=2,bytes=4=;countHardLimit=3;futureLimit=10"));
    }

    @Test
    public void testByteRoundTripUsesUtf8() {
        byte[] wire = "count=2,bytes=4=;limité=9".getBytes(StandardCharsets.UTF_8);
        StatsTrack stats = new StatsTrack(wire);
        assertArrayEquals(wire, stats.getStatsBytes());
        assertEquals(-1L, stats.getCountHardLimit());
        assertEquals(-1L, stats.getByteHardLimit());
    }
}
