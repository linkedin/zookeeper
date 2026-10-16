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

package org.apache.zookeeper.server;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.util.ServiceUtils;

/**
 * Recursively summarizes snapshot subtree data sizes and descendant counts.
 * Only non-leaf nodes are printed, but totals include all descendants and ephemeral nodes.
 * The maximum depth limits output, not traversal; zero means unlimited output depth.
 *
 * <p>Backported from Apache ZooKeeper commit 05b215994f5e145c2758c4089828b57ba471b329
 * (ZOOKEEPER-4566).
 */
@InterfaceAudience.Public
public class SnapshotRecursiveSummary {

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println(getUsage());
            ServiceUtils.requestSystemExit(ExitCode.INVALID_INVOCATION.getValue());
            return;
        }
        try {
            new SnapshotRecursiveSummary().run(args[0], args[1], Integer.parseInt(args[2]));
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            System.err.println(getUsage());
            ServiceUtils.requestSystemExit(ExitCode.INVALID_INVOCATION.getValue());
        } catch (IOException e) {
            System.err.println("Unable to read snapshot: " + e.getMessage());
            ServiceUtils.requestSystemExit(ExitCode.UNEXPECTED_ERROR.getValue());
        }
    }

    public void run(String snapshotFileName, String startingNode, int maxDepth) throws IOException {
        PathUtils.validatePath(startingNode);
        if (maxDepth < 0) {
            throw new IllegalArgumentException("max_depth must be a non-negative integer.");
        }
        String error = ZKUtil.validateFileInput(snapshotFileName);
        if (error != null) {
            throw new IllegalArgumentException(error);
        }
        DataTree dataTree = SnapshotComparer.getSnapshot(new File(snapshotFileName));
        if (dataTree.getNode(startingNode) == null) {
            throw new IllegalArgumentException("Starting node does not exist: " + startingNode);
        }
        StringBuilder builder = new StringBuilder();
        printZnode(dataTree, startingNode, builder, 0, maxDepth);
        System.out.println(builder);
    }

    private long[] printZnode(DataTree dataTree, String name, StringBuilder builder, int level, int maxDepth) {
        DataNode node = dataTree.getNode(name);
        Set<String> children;
        long dataSize;
        synchronized (node) {
            dataSize = node.data == null ? 0 : node.data.length;
            children = new TreeSet<>(node.getChildren());
        }
        long[] result = {1L, dataSize};
        if (children.isEmpty()) {
            return result;
        }
        StringBuilder childBuilder = new StringBuilder();
        for (String child : children) {
            long[] childResult = printZnode(dataTree, name + (name.equals("/") ? "" : "/") + child,
                                            childBuilder, level + 1, maxDepth);
            result[0] += childResult[0];
            result[1] += childResult[1];
        }
        if (maxDepth == 0 || level <= maxDepth) {
            String indent = String.join("", Collections.nCopies(level, "--"));
            builder.append(indent).append(" ").append(name).append("\n");
            builder.append(indent).append("   children: ").append(result[0] - 1).append("\n");
            builder.append(indent).append("   data: ").append(result[1]).append("\n");
            builder.append(childBuilder);
        }
        return result;
    }

    public static String getUsage() {
        String newLine = System.lineSeparator();
        return String.join(newLine,
                           "USAGE:",
                           "",
                           "SnapshotRecursiveSummary  <snapshot_file>  <starting_node>  <max_depth>",
                           "",
                           "snapshot_file:    path to the zookeeper snapshot",
                           "starting_node:    the absolute path in the zookeeper tree where traversal should begin",
                           "max_depth:        non-negative output depth. 0 displays every non-leaf node; "
                           + "1 displays the starting node and its non-leaf children; 2 adds another level, and so on. "
                           + "This ONLY affects the level of details displayed, NOT the calculation.");
    }

}
