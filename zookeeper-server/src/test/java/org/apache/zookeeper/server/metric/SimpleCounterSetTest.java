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

package org.apache.zookeeper.server.metric;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.metrics.CounterSet;
import org.apache.zookeeper.metrics.MetricsContext;
import org.apache.zookeeper.metrics.MetricsProvider;
import org.apache.zookeeper.metrics.impl.DefaultMetricsProvider;
import org.apache.zookeeper.metrics.impl.NullMetricsProvider;
import org.junit.Test;

public class SimpleCounterSetTest extends ZKTestCase {

    @Test
    public void testValuesAndReset() {
        SimpleCounterSet counterSet = new SimpleCounterSet("quota_exceeded_error_per_namespace");
        assertTrue(counterSet.values().isEmpty());

        counterSet.inc("apps");
        counterSet.add("apps", 2);
        counterSet.add("services", 4);

        Map<String, Object> expected = new HashMap<>();
        expected.put("apps_quota_exceeded_error_per_namespace", 3L);
        expected.put("services_quota_exceeded_error_per_namespace", 4L);
        assertEquals(expected, counterSet.values());

        counterSet.reset();
        expected.put("apps_quota_exceeded_error_per_namespace", 0L);
        expected.put("services_quota_exceeded_error_per_namespace", 0L);
        assertEquals(expected, counterSet.values());

        counterSet.inc("apps");
        expected.put("apps_quota_exceeded_error_per_namespace", 1L);
        assertEquals(expected, counterSet.values());
    }

    @Test
    public void testConcurrentUpdates() throws Exception {
        SimpleCounterSet counterSet = new SimpleCounterSet("errors");
        ExecutorService executor = Executors.newFixedThreadPool(4);
        CountDownLatch ready = new CountDownLatch(4);
        CountDownLatch start = new CountDownLatch(1);
        Future<?>[] updates = new Future<?>[4];
        try {
            for (int i = 0; i < updates.length; i++) {
                updates[i] = executor.submit(() -> {
                    ready.countDown();
                    start.await();
                    for (int j = 0; j < 1000; j++) {
                        counterSet.inc("apps");
                        counterSet.add("services", 2);
                    }
                    return null;
                });
            }
            assertTrue("Counter writers did not become ready", ready.await(10, TimeUnit.SECONDS));
            start.countDown();
            for (Future<?> update : updates) {
                update.get(10, TimeUnit.SECONDS);
            }

            Map<String, Object> expected = new HashMap<>();
            expected.put("apps_errors", 4000L);
            expected.put("services_errors", 8000L);
            assertEquals(expected, counterSet.values());
        } finally {
            start.countDown();
            executor.shutdownNow();
            assertTrue("Counter writers did not stop", executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testDefaultProviderCounterSetsAndReset() {
        MetricsProvider provider = new DefaultMetricsProvider();
        MetricsContext context = provider.getRootContext();
        CounterSet counterSet = context.getCounterSet("quota_exceeded_error_per_namespace");
        counterSet.inc("apps");
        counterSet.add("apps", 2);
        counterSet.add("services", 4);
        context.getCounterSet("other_errors").add("apps", 5);
        context.getCounter("requests").add(9);
        context.registerGauge("connections", () -> 7);
        context.registerGaugeSet("quota_limit", () -> Collections.singletonMap("apps", 42));

        Map<String, Object> expected = new HashMap<>();
        expected.put("apps_quota_exceeded_error_per_namespace", 3L);
        expected.put("services_quota_exceeded_error_per_namespace", 4L);
        expected.put("apps_other_errors", 5L);
        expected.put("requests", 9L);
        expected.put("connections", 7);
        expected.put("apps_quota_limit", 42);
        assertEquals(expected, dump(provider));
        assertSame(counterSet, context.getCounterSet("quota_exceeded_error_per_namespace"));

        provider.resetAllValues();
        expected.put("apps_quota_exceeded_error_per_namespace", 0L);
        expected.put("services_quota_exceeded_error_per_namespace", 0L);
        expected.put("apps_other_errors", 0L);
        expected.put("requests", 0L);
        assertEquals(expected, dump(provider));

        counterSet.inc("apps");
        expected.put("apps_quota_exceeded_error_per_namespace", 1L);
        assertEquals(expected, dump(provider));
        provider.stop();
    }

    @Test
    public void testDefaultProviderGaugeSetLifecycle() {
        MetricsProvider provider = new DefaultMetricsProvider();
        MetricsContext context = provider.getRootContext();
        Map<String, Number> values = new HashMap<>();
        values.put("apps", 10L);
        values.put("unset", null);
        values.put(null, 50L);
        AtomicInteger originalCalls = new AtomicInteger();
        AtomicInteger replacementCalls = new AtomicInteger();
        context.registerGaugeSet("quota_limit", () -> {
            originalCalls.incrementAndGet();
            return values;
        });
        assertEquals(0, originalCalls.get());

        Map<String, Object> expected = new HashMap<>();
        expected.put("apps_quota_limit", 10L);
        expected.put("unset_quota_limit", 0);
        assertEquals(expected, dump(provider));
        assertEquals(1, originalCalls.get());

        context.registerGaugeSet("quota_limit", () -> {
            replacementCalls.incrementAndGet();
            return Collections.singletonMap("apps", 30L);
        });
        assertEquals(Collections.singletonMap("apps_quota_limit", 30L), dump(provider));
        assertEquals(1, originalCalls.get());
        assertEquals(1, replacementCalls.get());

        context.unregisterGaugeSet("quota_limit");
        context.unregisterGaugeSet("quota_limit");
        assertTrue(dump(provider).isEmpty());
        assertEquals(1, replacementCalls.get());

        context.registerGaugeSet("quota_limit", () -> {
            fail("Stopped provider must release gauge set callbacks");
            return Collections.emptyMap();
        });
        context.registerGauge("connections", () -> {
            fail("Stopped provider must release gauge callbacks");
            return 0;
        });
        provider.stop();
        assertTrue(dump(provider).isEmpty());
    }

    @Test(expected = NullPointerException.class)
    public void testDefaultProviderRejectsNullGaugeSet() {
        new DefaultMetricsProvider().getRootContext().registerGaugeSet("quota_limit", null);
    }

    @Test
    public void testNullProviderMetricSets() {
        MetricsProvider provider = new NullMetricsProvider();
        MetricsContext context = provider.getRootContext();
        CounterSet counterSet = context.getCounterSet("errors");
        counterSet.inc("apps");
        counterSet.add("services", 2);
        context.registerGaugeSet("quota_limit", () -> {
            fail("Null provider must not sample gauge sets");
            return Collections.emptyMap();
        });
        assertTrue(dump(provider).isEmpty());
        provider.resetAllValues();
        context.unregisterGaugeSet("quota_limit");
        provider.stop();
        assertTrue(dump(provider).isEmpty());
    }

    private Map<String, Object> dump(MetricsProvider provider) {
        Map<String, Object> values = new HashMap<>();
        provider.dump(values::put);
        return values;
    }

}
