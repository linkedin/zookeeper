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

package org.apache.jute;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

public class BinaryOutputArchiveTest {

    private static int writtenSize(String s) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        BinaryOutputArchive.getArchive(new DataOutputStream(bytes)).writeString(s, "s");
        return bytes.size();
    }

    @Test
    public void testSerializedStringByteSizeMatchesWriteString() throws IOException {
        String[] values = {
            "",
            "/",
            "/ephemeral-0000000001",
            "/caf\u00e9/\u00df",
            "/\u4e2d\u6587/\u0800\uffef",
            "/surrogate-\ud83d\ude00",
        };
        for (String value : values) {
            assertEquals(writtenSize(value), BinaryOutputArchive.getSerializedStringByteSize(value), value);
        }
    }

    @Test
    public void testSerializedStringByteSizeCountsUtf8BytesNotChars() {
        String path = "/\u00e9\u4e2d";
        assertEquals(3, path.length());
        assertEquals(4 + 1 + 2 + 3, BinaryOutputArchive.getSerializedStringByteSize(path));
        assertEquals(path.getBytes(StandardCharsets.UTF_8).length + 4,
                BinaryOutputArchive.getSerializedStringByteSize(path));
    }
}
