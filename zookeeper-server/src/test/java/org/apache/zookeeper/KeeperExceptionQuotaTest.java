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
import static org.junit.Assert.assertTrue;
import org.apache.zookeeper.cli.CliWrapperException;
import org.junit.Test;

public class KeeperExceptionQuotaTest extends ZKTestCase {

    @Test
    public void testNativeQuotaErrorDecoding() {
        KeeperException error = KeeperException.create(KeeperException.Code.get(-125), "/limited");
        assertEquals("QuotaExceededException", error.getClass().getSimpleName());
        assertEquals("/limited", error.getPath());
        assertEquals("Quota has exceeded : /limited", new CliWrapperException(error).getMessage());
    }

    @Test
    public void testEphemeralErrorDecoding() {
        KeeperException error = KeeperException.create(KeeperException.Code.get(-128), "/session");
        assertTrue(error instanceof KeeperException.TotalEphemeralLimitExceeded);
        assertEquals("/session", error.getPath());
    }
}
