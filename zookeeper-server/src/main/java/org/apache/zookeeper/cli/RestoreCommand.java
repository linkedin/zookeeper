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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.commons.cli.PosixParser;
import org.apache.zookeeper.server.backup.RestoreFromBackupTool;

/**
 * Restore command for ZkCli.
 */
public class RestoreCommand extends CliCommand {

  private RestoreFromBackupTool tool;
  private CommandLine cl;
  private static Options options = new Options();
  private static final String RESTORE_ZXID_STR = "restore_zxid";
  private static final String RESTORE_TIMESTAMP_STR = "restore_timestamp";
  private static final String BACKUP_STORE_STR = "backup_store";
  private static final String SNAP_DESTINATION_STR = "snap_destination";
  private static final String LOG_DESTINATION_STR = "log_destination";
  private static final String TIMETABLE_STORAGE_PATH_STR = "timetable_storage_path";
  private static final String LOCAL_RESTORE_TEMP_DIR_PATH_STR = "local_restore_temp_dir_path";
  private static final String DRY_RUN_STR = "dry_run";
  public static final String RESTORE_ZXID_OPTION = "z";
  public static final String RESTORE_TIMESTAMP_OPTION = "t";
  public static final String BACKUP_STORE_OPTION = "b";
  public static final String SNAP_DESTINATION_OPTION = "s";
  public static final String LOG_DESTINATION_OPTION = "l";
  public static final String TIMETABLE_STORAGE_PATH_OPTION = "m";
  public static final String LOCAL_RESTORE_TEMP_DIR_PATH_OPTION = "r";
  public static final String DRY_RUN_OPTION = "n";
  private static final String RESTORE_ZXID_CMD = "-" + RESTORE_ZXID_OPTION + " " + RESTORE_ZXID_STR;
  private static final String RESTORE_TIMESTAMP_CMD =
      "-" + RESTORE_TIMESTAMP_OPTION + " " + RESTORE_TIMESTAMP_STR;
  private static final String BACKUP_STORE_CMD = "-" + BACKUP_STORE_OPTION + " " + BACKUP_STORE_STR;
  private static final String SNAP_DESTINATION_CMD =
      "-" + SNAP_DESTINATION_OPTION + " " + SNAP_DESTINATION_STR;
  private static final String LOG_DESTINATION_CMD =
      "-" + LOG_DESTINATION_OPTION + " " + LOG_DESTINATION_STR;
  private static final String TIMETABLE_STORAGE_PATH_CMD =
      "-" + TIMETABLE_STORAGE_PATH_OPTION + " " + TIMETABLE_STORAGE_PATH_STR;
  private static final String LOCAL_RESTORE_TEMP_DIR_PATH_CMD =
      "-" + LOCAL_RESTORE_TEMP_DIR_PATH_OPTION + " " + LOCAL_RESTORE_TEMP_DIR_PATH_STR;
  private static final String DRY_RUN_CMD = "-" + DRY_RUN_OPTION;
  private static final String RESTORE_CMD_STR = "restore";
  private static final String OPTION_STR =
      "[" + RESTORE_ZXID_CMD + "]/[" + RESTORE_TIMESTAMP_CMD + "] [" + BACKUP_STORE_CMD + "] ["
          + SNAP_DESTINATION_CMD + "] [" + LOG_DESTINATION_CMD + "] [" + TIMETABLE_STORAGE_PATH_CMD
          + "](needed if restore to a timestamp) [" + LOCAL_RESTORE_TEMP_DIR_PATH_CMD
          + "](optional) [" + DRY_RUN_CMD + "](optional)";

  static {
    options.addOption(new Option(RESTORE_ZXID_OPTION, true, RESTORE_ZXID_STR));
    options.addOption(new Option(RESTORE_TIMESTAMP_OPTION, true, RESTORE_TIMESTAMP_STR));
    options.addOption(new Option(BACKUP_STORE_OPTION, true, BACKUP_STORE_STR));
    options.addOption(new Option(SNAP_DESTINATION_OPTION, true, SNAP_DESTINATION_STR));
    options.addOption(new Option(LOG_DESTINATION_OPTION, true, LOG_DESTINATION_STR));
    options.addOption(new Option(TIMETABLE_STORAGE_PATH_OPTION, true, TIMETABLE_STORAGE_PATH_STR));
    options.addOption(
        new Option(LOCAL_RESTORE_TEMP_DIR_PATH_OPTION, true, LOCAL_RESTORE_TEMP_DIR_PATH_STR));
    options.addOption(new Option(DRY_RUN_OPTION, false, DRY_RUN_STR));
  }

  public RestoreCommand() {
    super(RESTORE_CMD_STR, OPTION_STR);
    tool = new RestoreFromBackupTool();
  }

  @Override
  public String getUsageStr() {
    return "Usage: RestoreFromBackupTool  " + RESTORE_CMD_STR + " " + OPTION_STR + "\n    "
        + RESTORE_ZXID_CMD
        + ": the point to restore to, either the string 'latest' or a zxid in hex format. Choose one between this option or "
        + RESTORE_TIMESTAMP_CMD + ", if both are specified, this option will be prioritized\n    "
        + RESTORE_TIMESTAMP_CMD
        + ": the point to restore to, a timestamp in long format. Choose one between this option or "
        + RESTORE_ZXID_CMD + ".\n    " + BACKUP_STORE_CMD
        + ": the connection information for the backup store\n           For GPFS the format is: gpfs:<config_path>:<backup_path>:<namespace>\n    "
        + SNAP_DESTINATION_CMD + ": local destination path for restored snapshots\n    "
        + LOG_DESTINATION_CMD + ": local destination path for restored txlogs\n    "
        + TIMETABLE_STORAGE_PATH_CMD
        + ": Needed if restore to a timestamp. Backup storage path for timetable files, for GPFS the format is: gpfs:<config_path>:<backup_path>:<namespace>, if not set, default to be same as backup storage path\n    "
        + LOCAL_RESTORE_TEMP_DIR_PATH_CMD
        + ": Optional, local path for creating a temporary intermediate directory for restoration, the directory will be deleted after restoration is done\n    "
        + DRY_RUN_CMD + " " + DRY_RUN_STR
        + ": Optional, no files will be actually copied in a dry run";
  }

  @Override
  public CliCommand parse(String[] cmdArgs) throws CliParseException {
    Parser parser = new PosixParser();
    try {
      cl = parser.parse(options, cmdArgs);
    } catch (ParseException ex) {
      throw new CliParseException(getUsageStr(), ex);
    }
    if ((!cl.hasOption(RESTORE_ZXID_OPTION) && !cl.hasOption(RESTORE_TIMESTAMP_OPTION)) || !cl
        .hasOption(BACKUP_STORE_OPTION) || !cl.hasOption(SNAP_DESTINATION_OPTION) || !cl
        .hasOption(LOG_DESTINATION_OPTION)) {
      throw new CliParseException("Missing required argument(s).\n" + getUsageStr());
    }
    return this;
  }

  @Override
  public boolean exec() throws CliException {
    return tool.runWithRetries(cl);
  }
}