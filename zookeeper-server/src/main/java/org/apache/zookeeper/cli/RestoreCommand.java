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


package org.apache.zookeeper.cli;

import org.apache.zookeeper.common.ConfigException;
import org.apache.zookeeper.server.backup.RestoreFromBackupTool;

/**
 * Restore command for ZkCli.
 */
public class RestoreCommand extends CliCommand {

  private static final int RESTORE_NUM_ARGS_REQUIRED = 4;

  private RestoreFromBackupTool tool;


  public RestoreCommand() {
    super("restore",
        "<restore point (zxid)> "
            + "<backup storage type:storage config type:backup path:namespace> "
            + "<snapshot destination path> "
            + "<log destination path> "
            + "<-n:dry run>");
    tool = new RestoreFromBackupTool();
  }

  @Override
  public CliCommand parse(String[] cmdArgs) throws CliParseException {
    try {
      tool.parseArgs(cmdArgs);
    } catch (ConfigException e) {
      throw new CliParseException(e.getMessage());
    }
    return this;
  }

  @Override
  public boolean exec() throws CliException {
    return tool.runWithRetries();
  }
}
