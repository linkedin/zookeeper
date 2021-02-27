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

package org.apache.zookeeper.server.backup;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import jline.internal.Log;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.backup.BackupUtil.BackupFileType;
import org.apache.zookeeper.server.backup.BackupUtil.ZxidPart;
import org.apache.zookeeper.server.backup.exception.RestoreException;
import org.apache.zookeeper.server.backup.storage.BackupStorageProvider;
import org.apache.zookeeper.server.backup.storage.BackupStorageUtil;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.persistence.Util;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  This is a tool to restore, from backup storage, a snapshot and set of transaction log files
 *  that combine to represent a transactionally consistent image of a ZooKeeper ensemble at
 *  some zxid.
 */
public class RestoreFromBackupTool {
  private static final Logger LOG = LoggerFactory.getLogger(RestoreFromBackupTool.class);

  BackupStorageProvider storage;
  FileTxnSnapLog snapLog;
  long zxidToRestore;
  boolean dryRun;

  List<BackupFileInfo> logs;
  List<BackupFileInfo> snaps;
  List<BackupFileInfo> filesToCopy;

  int mostRecentLogNeededIndex;
  int snapNeededIndex;
  int oldestLogNeededIndex;

  /**
   * Default constructor; requires using parseArgs to setup state.
   */
  public RestoreFromBackupTool() {
    this(null, null, -1L, false);
  }

  /**
   * Constructor
   * @param storageProvider the backup provider from which to restore the backups
   * @param snapLog the snap and log provider to use
   * @param zxidToRestore the zxid upto which to restore, or Long.MAX to restore to the latest
   *                      available transactionally consistent zxid.
   * @param dryRun whether this is a dryrun in which case no files are actually copied
   */
  RestoreFromBackupTool(BackupStorageProvider storageProvider, FileTxnSnapLog snapLog,
      long zxidToRestore, boolean dryRun) {

    filesToCopy = new ArrayList<BackupFileInfo>();
    snapNeededIndex = -1;
    mostRecentLogNeededIndex = -1;
    oldestLogNeededIndex = -1;

    this.storage = storageProvider;
    this.zxidToRestore = zxidToRestore;
    this.snapLog = snapLog;
    this.dryRun = dryRun;
  }

  /**
   * Attempts to perform a restore.
   */
  public void run() throws IOException {
    if (!findFilesToRestore()) {
      throw new RestoreException("Failed to find a valid snapshot and logs to restore.");
    }

    // Create a temporary dir in backup storage for backup file processing
    File tempDirForRestoration =
        new File(filesToCopy.listIterator().next().getBackedUpFile().getParent(),
            "Restore_" + zxidToRestore);

    try {
      storage.processBackupFilesForRestoration(tempDirForRestoration, filesToCopy, zxidToRestore);
      Log.info("Copied " + filesToCopy.size() + " backup files to temp dir " + tempDirForRestoration
          .getPath() + " in backup storage for file preparation for restoration.");

      Collection<File> processedFiles = FileUtils.listFiles(tempDirForRestoration, null, true);
      for (File processedFile : processedFiles) {
        if (processedFile.isDirectory()) {
          continue;
        }
        String fileName = processedFile.getName();
        File parentFile = processedFile.getParentFile();
        File base = Util.isSnapshotFileName(fileName) ? snapLog.getSnapDir() : snapLog.getDataDir();
        File destination = new File(base, fileName);

        if (!destination.exists()) {
          LOG.info("Copying " + fileName + " to " + destination.getPath() + ".");

          if (!dryRun) {
            // Adding "RESTORE_" prefix to file name to differentiate the restore files from original backup files,
            // so in storage side it will use a different way to construct the file path from original backup files
            storage.copyToLocalStorage(
                new File(parentFile, BackupStorageUtil.RESTORE_FILE_PREFIX + fileName),
                destination);
          }
        } else {
          LOG.info("Skipping " + fileName + " because it already exists as " + destination.getPath()
              + ".");
        }
      }
    } catch (IOException ioe) {
      throw new IOException("Hit exception when attempting to restore." + ioe.getMessage());
    } finally {
      storage.cleanupTempFilesForRestoration(tempDirForRestoration);
    }
  }

  /**
   * Finds the set of files (snapshot and txlog) that are required to restore to a transactionally
   * consistent point for the requested zxid.
   * Note that when restoring to the latest zxid, the transactionally consistent point may NOT
   * be the latest backed up zxid if logs have been lost in between; or if there is no
   * transactionally consistent point in which nothing will be restored but the restored will
   * be considered successful.
   * @return true if a transactionally consistent set of files could be found for the requested
   *         restore point; false in all other cases.
   * @throws IOException
   */
  private boolean findFilesToRestore() throws IOException {
    snaps = BackupUtil.getBackupFiles(storage, BackupFileType.SNAPSHOT, ZxidPart.MIN_ZXID,
        BackupUtil.SortOrder.DESCENDING);
    logs = BackupUtil.getBackupFiles(storage,
        new BackupFileType[]{BackupFileType.TXNLOG, BackupFileType.LOSTLOG}, ZxidPart.MIN_ZXID,
        BackupUtil.SortOrder.DESCENDING);
    filesToCopy = new ArrayList<>();

    snapNeededIndex = -1;

    while (findNextPossibleSnapshot()) {
      if (findLogRange()) {
        filesToCopy.add(snaps.get(snapNeededIndex));
        filesToCopy.addAll(logs.subList(mostRecentLogNeededIndex, oldestLogNeededIndex + 1));
        return true;
      }

      if (zxidToRestore != Long.MAX_VALUE) {
        break;
      }
    }

    // Restoring to an empty data tree (i.e. no backup files) is valid for restoring to
    // latest
    if (zxidToRestore == Long.MAX_VALUE) {
      return true;
    }

    return false;
  }

  /**
   * Find the next snapshot whose range is below the requested restore point.
   * Note: in practice this only gets called once when zxidToRestore != Long.MAX
   * @return true if a snapshot is found; false in all other cases.
   */
  private boolean findNextPossibleSnapshot() {
    for (snapNeededIndex++; snapNeededIndex < snaps.size(); snapNeededIndex++) {
      if (snaps.get(snapNeededIndex).getZxidRange().upperEndpoint() <= zxidToRestore) {
        return true;
      }
    }

    return false;
  }

  /**
   * Find the range of log files needed to make the current proposed snapshot transactionally
   * consistent to the requested zxid.
   * When zxidToRestore == Long.MAX the transaction log range terminates either on the most
   * recent backedup txnlog OR the last txnlog prior to a lost log range (assuming that log
   * still makes the snapshot transactionally consistent).
   * @return true if a valid log range is found; false in all other cases.
   */
  private boolean findLogRange() {
    Preconditions.checkState(snapNeededIndex >= 0 && snapNeededIndex < snaps.size());

    if (logs.size() == 0) {
      return false;
    }

    BackupFileInfo snap = snaps.get(snapNeededIndex);
    Range<Long> snapRange = snap.getZxidRange();
    oldestLogNeededIndex = logs.size() - 1;
    mostRecentLogNeededIndex = 0;

    // Find the first txlog that might make the snapshot valid, OR is a lost log
    for (int i = 0; i < logs.size() - 1; i++) {
      BackupFileInfo log = logs.get(i);
      Range<Long> logRange = log.getZxidRange();

      if (logRange.lowerEndpoint() <= snapRange.lowerEndpoint()) {
        oldestLogNeededIndex = i;
        break;
      }
    }

    // Starting if the oldest log that might allow the snapshot to be valid, find the txnlog
    // that includes the restore point, OR is a lost log
    for (int i = oldestLogNeededIndex; i > 0; i--) {
      BackupFileInfo log = logs.get(i);
      Range<Long> logRange = log.getZxidRange();

      if (log.getFileType() == BackupFileType.LOSTLOG
          || logRange.upperEndpoint() >= zxidToRestore) {

        mostRecentLogNeededIndex = i;
        break;
      }
    }

    return validateLogRange();
  }

  private boolean validateLogRange() {
    Preconditions.checkState(oldestLogNeededIndex >= 0);
    Preconditions.checkState(oldestLogNeededIndex < logs.size());
    Preconditions.checkState(mostRecentLogNeededIndex >= 0);
    Preconditions.checkState(mostRecentLogNeededIndex < logs.size());
    Preconditions.checkState(oldestLogNeededIndex >= mostRecentLogNeededIndex);
    Preconditions.checkState(snapNeededIndex >= 0);
    Preconditions.checkState(snapNeededIndex < snaps.size());

    BackupFileInfo snap = snaps.get(snapNeededIndex);
    BackupFileInfo oldestLog = logs.get(oldestLogNeededIndex);
    BackupFileInfo newestLog = logs.get(mostRecentLogNeededIndex);

    if (oldestLog.getFileType() == BackupFileType.LOSTLOG) {
      LOG.error("Could not find logs to make the snapshot '" + snap.getBackedUpFile()
          + "' valid. Lost logs at " + logs.get(oldestLogNeededIndex).getZxidRange());
      return false;
    }

    if (newestLog.getFileType() == BackupFileType.LOSTLOG) {
      if (zxidToRestore == Long.MAX_VALUE && oldestLogNeededIndex != mostRecentLogNeededIndex) {
        // When restoring to the latest, we can use the last valid log prior to lost log
        // range.
        mostRecentLogNeededIndex++;
      } else {
        LOG.error("Could not find logs to make the snapshot '" + snap.getBackedUpFile()
            + "' valid. Lost logs at " + logs.get(mostRecentLogNeededIndex).getZxidRange() + ".");
        return false;
      }
    }

    Range<Long> fullRange = oldestLog.getZxidRange().span(newestLog.getZxidRange());

    if (fullRange.lowerEndpoint() > snap.getZxidRange().lowerEndpoint()) {
      LOG.error("Could not find logs to make snap '" + snap.getBackedUpFile()
          + "' valid. Logs start at zxid " + ZxidUtils.zxidToString(fullRange.lowerEndpoint())
          + ".");
      return false;
    }

    if (fullRange.upperEndpoint() < snap.getZxidRange().upperEndpoint()) {
      LOG.error("Could not find logs to make snap '" + snap.getBackedUpFile()
          + "' valid. Logs end at zxid " + ZxidUtils.zxidToString(fullRange.upperEndpoint()) + ".");
      return false;
    }

    if (zxidToRestore != Long.MAX_VALUE && fullRange.upperEndpoint() < zxidToRestore) {
      LOG.error("Could not find logs to restore to zxid " + zxidToRestore + ". Logs end at zxid "
          + ZxidUtils.zxidToString(fullRange.upperEndpoint()) + ".");
      return false;
    }

    return true;
  }
}
