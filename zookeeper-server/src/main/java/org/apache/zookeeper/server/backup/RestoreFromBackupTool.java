package org.apache.zookeeper.server.backup;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.common.ConfigException;
import org.apache.zookeeper.server.backup.storage.BackupStorageProvider;
import org.apache.zookeeper.server.backup.storage.impl.FileSystemBackupStorage;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

/**
 * TODO: This is not a complete implementation.
 * RestoreFromBackupTool skeleton.
 */
public class RestoreFromBackupTool {
  private static final int MAX_RETRIES = 10;

  BackupStorageProvider storage;

  FileTxnSnapLog snapLog;
  long zxidToRestore;
  boolean dryRun = false;

  List<BackupFileInfo> logs;
  List<BackupFileInfo> snaps;
  List<BackupFileInfo> filesToCopy;

  int mostRecentLogNeededIndex;
  int snapNeededIndex;
  int oldestLogNeededIndex;

  public static void usage() {
    System.out.println("Usage: RestoreFromBackupTool restore <restore_point> <backup_store> <data_destination> <log_destination>");
    System.out.println("    restore_point: the point to restore to, either the string 'latest' or a zxid in hex format.");
    System.out.println("    backup_store: the connection information for the backup store");
    System.out.println("       For GPFS the format is: gpfs:<config_path>:<backup_path>:<namespace>");
    System.out.println("           config_path: the path to the hdfs configuration");
    System.out.println("           backup_path: the path within hdfs where the backups are stored");
    System.out.println("    data_destination: local destination path for restored snapshots");
    System.out.println("    log_destination: local destination path for restored txlogs");
  }

  /**
   * Parse and validate arguments to the tool
   * @param args the set of arguments
   * @return true if the arguments parse correctly; false in all other cases.
   * @throws IOException if the backup provider cannot be instantiated correctly.
   */
  public void parseArgs(String[] args) throws ConfigException {
    // Check the num of args
    if (args.length != 4 && args.length != 5) {
      System.err.println("Invalid number of arguments for restore command");
      usage();
      System.exit(3);
    }

    // Read the restore point
    if (args[0].equals("latest")) {
      zxidToRestore = Long.MAX_VALUE;
    } else {
      int base = 10;
      String numStr = args[0];

      if (args[0].startsWith("0x")) {
        numStr = args[0].substring(2);
        base = 16;
      }

      try {
        zxidToRestore = Long.parseLong(numStr, base);
      } catch (NumberFormatException nfe) {
        System.err.println("Invalid number specified for restore point");
        usage();
        System.exit(4);
      }
    }

    // Read the storage provider args
    String[] providerParts = args[2].split(":");

    if (providerParts.length != 4 || !providerParts[0].equals("gpfs")) {
      // TODO: Support other storage implementations when making this OSS-compatible
      System.err.println("GPFS is the only backup provider supported or the specification is incorrect.");
      usage();
      System.exit(5);
    }

    // Check if this is a dry-run
    if (args.length == 5) {
      if (!args[4].equals("-n")) {
        System.err.println("Invalid argument: " + args[4]);
        usage();
        System.exit(6);
      }

      dryRun = true;
    }

    // TODO: Construct a BackupConfig
    BackupConfig backupConfig = new BackupConfig.Builder().build().get();

    storage = new FileSystemBackupStorage(backupConfig);

    try {
      File snapDir = new File(args[3]);
      File logDir = new File(args[4]);
      snapLog = new FileTxnSnapLog(logDir, snapDir);
    } catch (IOException ioe) {
      System.err.println("Could not setup transaction log utility." + ioe);
      System.exit(8);
    }

    System.out.println("parseArgs successful.");
  }

  public boolean runWithRetries() {
    return true;
  }
}
