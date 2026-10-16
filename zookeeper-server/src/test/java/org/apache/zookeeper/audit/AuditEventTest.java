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
import static org.junit.Assert.assertFalse;
import java.util.Locale;
import org.apache.zookeeper.audit.AuditEvent.Result;
import org.junit.Test;

public class AuditEventTest {

    @Test
    public void testFormat() {
        AuditEvent auditEvent = new AuditEvent(Result.SUCCESS);
        auditEvent.addEntry(AuditEvent.FieldName.USER, "Value1");
        auditEvent.addEntry(AuditEvent.FieldName.OPERATION, "Value2");
        String actual = auditEvent.toString();
        String expected = "user=Value1\toperation=Value2\tresult=success";
        assertEquals(expected, actual);
    }

    @Test
    public void testFormatShouldIgnoreKeyIfValueIsNull() {
        AuditEvent auditEvent = new AuditEvent(Result.SUCCESS);
        auditEvent.addEntry(AuditEvent.FieldName.USER, null);
        auditEvent.addEntry(AuditEvent.FieldName.OPERATION, "Value2");
        String actual = auditEvent.toString();
        String expected = "operation=Value2\tresult=success";
        assertEquals(expected, actual);
    }

    @Test
    public void testProtocolNamesDoNotDependOnDefaultLocale() {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(new Locale("tr", "TR"));
            AuditEvent event = new AuditEvent(Result.FAILURE);
            event.addEntry(AuditEvent.FieldName.IP, "127.0.0.1");
            assertEquals("ip=127.0.0.1\tresult=failure", event.toString());
            assertEquals("result=invoked", new AuditEvent(Result.INVOKED).toString());
        } finally {
            Locale.setDefault(previous);
        }
    }

    @Test
    public void testEnhancedFormattingEscapesValuesReversibly() {
        String previousAudit = System.getProperty(ZKAuditProvider.AUDIT_ENABLE);
        String previousEnhanced = System.getProperty(AuditHelperTest.ENHANCED_ENABLE);
        try {
            System.setProperty(ZKAuditProvider.AUDIT_ENABLE, "true");
            System.setProperty(AuditHelperTest.ENHANCED_ENABLE, "true");
            AuditEvent event = ZKAuditProvider.createLogEvent("team\tname\r\n\\t", "setData",
                    "/name=value\\child", null, null, null, null, Result.FAILURE);
            String log = event.toString();
            assertEquals("2", AuditHelperTest.fields(log).get("schema_version"));
            assertEquals("team\\tname\\r\\n\\\\t", AuditHelperTest.fields(log).get("user"));
            assertEquals("/name=value\\\\child", AuditHelperTest.fields(log).get("znode"));
            assertEquals("team\tname\r\n\\t", event.getValue(AuditEvent.FieldName.USER));
            assertFalse(log.contains("\n"));
            assertFalse(log.contains("\r"));
        } finally {
            AuditHelperTest.restoreProperty(ZKAuditProvider.AUDIT_ENABLE, previousAudit);
            AuditHelperTest.restoreProperty(AuditHelperTest.ENHANCED_ENABLE, previousEnhanced);
        }
    }

    @Test
    public void testLegacyValuesAreNotEscaped() {
        AuditEvent event = new AuditEvent(Result.SUCCESS);
        event.addEntry(AuditEvent.FieldName.USER, "team\tname\n\\t");
        assertEquals("user=team\tname\n\\t\tresult=success", event.toString());
    }

    @Test
    public void testSchemaMarkerEscapesPreviouslyAddedValues() {
        AuditEvent event = new AuditEvent(Result.SUCCESS);
        event.addEntry(AuditEvent.FieldName.USER, "team\tname\n");
        event.addEntry(AuditEvent.FieldName.SCHEMA_VERSION, "2");
        assertEquals("user=team\\tname\\n\tschema_version=2\tresult=success", event.toString());
    }
}
