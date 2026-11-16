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

package org.apache.zookeeper.server.backup.monitoring;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wall-clock timestamp kept in a small file in the backup status directory so that it survives
 * server restarts. Failures to read or write are logged and never interrupt the backup.
 */
class PersistedTimestamp {
  private static final Logger LOG = LoggerFactory.getLogger(PersistedTimestamp.class);

  private final Path dir;
  private final String fileName;

  /**
   * @param dir directory holding the file; null disables persistence
   * @param fileName name of the file inside {@code dir}
   */
  PersistedTimestamp(File dir, String fileName) {
    this.dir = dir == null ? null : dir.toPath();
    this.fileName = fileName;
  }

  /**
   * @return the stored timestamp, or {@code defaultValue} if none is stored or it cannot be read
   */
  long readOrDefault(long defaultValue) {
    if (dir == null) {
      return defaultValue;
    }
    Path file = dir.resolve(fileName);
    if (!Files.exists(file)) {
      return defaultValue;
    }
    try {
      return Long.parseLong(new String(Files.readAllBytes(file), StandardCharsets.UTF_8).trim());
    } catch (IOException | NumberFormatException e) {
      LOG.warn("Ignoring unreadable backup timestamp file {}", file, e);
      return defaultValue;
    }
  }

  void write(long timestamp) {
    if (dir == null) {
      return;
    }
    Path file = dir.resolve(fileName);
    Path tmp = dir.resolve(fileName + ".tmp");
    try {
      Files.createDirectories(dir);
      Files.write(tmp, Long.toString(timestamp).getBytes(StandardCharsets.UTF_8));
      try {
        Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
      } catch (AtomicMoveNotSupportedException e) {
        Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
      }
    } catch (IOException e) {
      LOG.warn("Failed to persist backup timestamp to {}", file, e);
    }
  }
}
