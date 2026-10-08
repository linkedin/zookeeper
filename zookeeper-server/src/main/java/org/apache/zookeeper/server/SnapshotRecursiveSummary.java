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
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
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
        Deque<StringBuilder> children = new ArrayDeque<>();
        walk(dataTree, startingNode, new TreeVisitor() {
            @Override
            public void visit(String path, DataNode node, int depth) {
                children.push(new StringBuilder());
            }

            @Override
            public void leave(String path, int depth, long nodes, long payloadBytes, long pathBytes) {
                StringBuilder childSummary = children.pop();
                StringBuilder summary = new StringBuilder();
                if (nodes > 1 && (maxDepth == 0 || depth <= maxDepth)) {
                    String indent = String.join("", Collections.nCopies(depth, "--"));
                    summary.append(indent).append(" ").append(path).append("\n");
                    summary.append(indent).append("   children: ").append(nodes - 1).append("\n");
                    summary.append(indent).append("   data: ").append(payloadBytes).append("\n");
                    summary.append(childSummary);
                }
                (children.isEmpty() ? builder : children.peek()).append(summary);
            }
        });
        System.out.println(builder);
    }

    interface TreeVisitor {

        void visit(String path, DataNode node, int depth) throws IOException;

        void leave(String path, int depth, long nodes, long payloadBytes, long pathBytes) throws IOException;

    }

    private static final class Frame {

        final String path;
        final int depth;
        final Iterator<String> children;
        long nodes = 1;
        long payloadBytes;
        long pathBytes;

        Frame(String path, int depth, DataNode node) {
            this.path = path;
            this.depth = depth;
            synchronized (node) {
                children = new TreeSet<>(node.getChildren()).iterator();
                payloadBytes = node.data == null ? 0 : node.data.length;
            }
            pathBytes = path.getBytes(StandardCharsets.UTF_8).length;
        }

    }

    /** Visits the offline tree once, with inclusive subtree totals on exit and no second tree. */
    static void walk(DataTree tree, String startingNode, TreeVisitor visitor) throws IOException {
        String start = startingNode.isEmpty() ? "/" : startingNode;
        Deque<Frame> stack = new ArrayDeque<>();
        enter(tree, start, 0, visitor, stack);
        while (!stack.isEmpty()) {
            Frame current = stack.peek();
            if (current.children.hasNext()) {
                String child = current.path + (current.path.equals("/") ? "" : "/") + current.children.next();
                enter(tree, child, current.depth + 1, visitor, stack);
            } else {
                stack.pop();
                visitor.leave(current.path, current.depth, current.nodes, current.payloadBytes, current.pathBytes);
                if (!stack.isEmpty()) {
                    Frame parent = stack.peek();
                    parent.nodes = Math.addExact(parent.nodes, current.nodes);
                    parent.payloadBytes = Math.addExact(parent.payloadBytes, current.payloadBytes);
                    parent.pathBytes = Math.addExact(parent.pathBytes, current.pathBytes);
                }
            }
        }
    }

    private static void enter(DataTree tree, String path, int depth, TreeVisitor visitor, Deque<Frame> stack)
        throws IOException {
        PathUtils.validatePath(path);
        DataNode node = tree.getNode(path);
        if (node == null) {
            throw new IOException("Missing snapshot node: " + path);
        }
        stack.push(new Frame(path, depth, node));
        visitor.visit(path, node, depth);
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
