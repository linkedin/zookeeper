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

package org.apache.zookeeper.server.backup.timetable;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.Map;
import java.util.TreeMap;

import org.apache.zookeeper.server.backup.exception.BackupException;

/**
 * Util methods used to operate on timetable backup.
 */
public class TimetableUtil {
  private static final String LATEST = "latest";

  /**
   * Returns the last zxid corresponding to the timestamp preceding the timestamp given as argument.
   * @param timetableBackupFiles
   * @param timestamp timestamp string (long), or "latest"
   * @return Hex String representation of the zxid found
   */
  public static String findLastZxidFromTimestamp(File[] timetableBackupFiles, String timestamp) {
    // Verify arguments: backup files and timestamp
    if (timetableBackupFiles == null || timetableBackupFiles.length == 0) {
      throw new IllegalArgumentException(
          "TimetableUtil::findLastZxidFromTimestamp(): timetableBackupFiles argument is either null"
              + " or empty!");
    }

    long timestampLong;
    if (!timestamp.equalsIgnoreCase(LATEST)) {
      try {
        timestampLong = Long.decode(timestamp);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "TimetableUtil::findLastZxidFromTimestamp(): cannot convert the given timestamp to a"
                + " valid long! timestamp: " + timestamp, e);
      }
    } else {
      timestampLong = Long.MAX_VALUE;
    }

    // Convert timetable backup files to an ordered Map<Long, String>, timestamp:zxid pairs
    TreeMap<Long, String> timestampZxidPairs = new TreeMap<>();
    try {
      for (File file : timetableBackupFiles) {
        FileInputStream fis = new FileInputStream(file);
        ObjectInputStream ois = new ObjectInputStream(fis);
        @SuppressWarnings("unchecked")
        Map<Long, String> map = (TreeMap<Long, String>) ois.readObject();
        timestampZxidPairs.putAll(map);
      }
    } catch (Exception e) {
      throw new BackupException(
          "TimetableUtil::findLastZxidFromTimestamp(): failed to read timetable backup files!", e);
    }

    // Check if the given timestamp is in range
    if (!timestamp.equalsIgnoreCase(LATEST) && (timestampLong < timestampZxidPairs.firstKey()
        || timestampLong > timestampZxidPairs.lastKey())) {
      throw new IllegalArgumentException(
          "TimetableUtil::findLastZxidFromTimestamp(): timestamp given is not in the timestamp "
              + "range given in the backup files!");
    }

    // Find the last zxid corresponding to the timestamp given
    Map.Entry<Long, String> floorEntry = timestampZxidPairs.floorEntry(timestampLong);
    if (floorEntry == null) {
      throw new BackupException("TimetableUtil::findLastZxidFromTimestamp(): floorEntry is null!");
    }
    return floorEntry.getValue();
  }
}
