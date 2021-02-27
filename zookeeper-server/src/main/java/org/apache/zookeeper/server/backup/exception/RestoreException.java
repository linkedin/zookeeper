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
package org.apache.zookeeper.server.backup.exception;

/**
 * Signals an out-of-range error has occurred during the ZooKeeper restore process.
 * For example: User requested to restore to a timestamp earlier than the earliest timestamp recorded in timetable,
 * or later than the latest timestamp recorded in timetable.
 * Or user requested a zxid point that cannot be restored using the backup files available in backup storage.
 */
public class RestoreException extends RuntimeException {
  public RestoreException(String message) {
    super(message);
  }

  public RestoreException(String message, Exception e) {
    super(message, e);
  }
}
