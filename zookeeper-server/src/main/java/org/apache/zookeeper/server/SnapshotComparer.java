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

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.zip.CheckedInputStream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.jute.BinaryInputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.server.persistence.FileHeader;
import org.apache.zookeeper.server.persistence.FileSnap;
import org.apache.zookeeper.server.persistence.SnapStream;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.util.ServiceUtils;

/**
 * Compares snapshot subtree data sizes and descendant counts, including ephemeral nodes.
 * Sessions, payload contents, ACLs and other metadata are not compared. Equal sizes and
 * counts do not establish identical contents or transaction-consistent state.
 *
 * <p>Backported from Apache ZooKeeper commit f90060b83da4bfcca58ada93a57fedb40a069387
 * (ZOOKEEPER-3427).
 */
public class SnapshotComparer {

    private static final String LEFT_OPTION = "left";
    private static final String RIGHT_OPTION = "right";
    private static final String BYTE_THRESHOLD_OPTION = "bytes";
    private static final String NODE_THRESHOLD_OPTION = "nodes";
    private static final String DEBUG_OPTION = "debug";
    private static final String INTERACTIVE_OPTION = "interactive";

    private final Options options = new Options();

    private SnapshotComparer() {
        options.addOption(Option.builder("l").longOpt(LEFT_OPTION).hasArg().required()
                          .argName("LEFT").desc("(Required) The left snapshot file.").build());
        options.addOption(Option.builder("r").longOpt(RIGHT_OPTION).hasArg().required()
                          .argName("RIGHT").desc("(Required) The right snapshot file.").build());
        options.addOption(Option.builder("b").longOpt(BYTE_THRESHOLD_OPTION).hasArg().required()
                          .argName("BYTETHRESHOLD")
                          .desc("(Required) The node data delta size threshold, in bytes, for printing the node.").build());
        options.addOption(Option.builder("n").longOpt(NODE_THRESHOLD_OPTION).hasArg().required()
                          .argName("NODETHRESHOLD")
                          .desc("(Required) The descendant node delta size threshold, in nodes, for printing the node.").build());
        options.addOption("d", DEBUG_OPTION, false, "Use debug output.");
        options.addOption("i", INTERACTIVE_OPTION, false, "Enter interactive mode.");
    }

    private void usage() {
        new HelpFormatter().printHelp(120, "java -cp <classPath> " + SnapshotComparer.class.getName(), "", options, "");
    }

    public static void main(String[] args) {
        new SnapshotComparer().compareSnapshots(args);
    }

    private void compareSnapshots(String[] args) {
        CommandLine parsedOptions;
        File left;
        File right;
        int byteThreshold;
        int nodeThreshold;
        try {
            parsedOptions = new DefaultParser().parse(options, args);
            if (!parsedOptions.getArgList().isEmpty()) {
                throw new IllegalArgumentException("Unexpected arguments: " + parsedOptions.getArgList());
            }
            byteThreshold = Integer.parseInt(parsedOptions.getOptionValue(BYTE_THRESHOLD_OPTION));
            nodeThreshold = Integer.parseInt(parsedOptions.getOptionValue(NODE_THRESHOLD_OPTION));
            if (byteThreshold < 0 || nodeThreshold < 0) {
                throw new IllegalArgumentException("Thresholds must be non-negative integers.");
            }
            left = new File(parsedOptions.getOptionValue(LEFT_OPTION));
            right = new File(parsedOptions.getOptionValue(RIGHT_OPTION));
            for (File file : new File[]{left, right}) {
                String error = ZKUtil.validateFileInput(file.toString());
                if (error != null) {
                    throw new IllegalArgumentException(error);
                }
            }
        } catch (ParseException | IllegalArgumentException e) {
            System.err.println(e.getMessage());
            usage();
            ServiceUtils.requestSystemExit(ExitCode.INVALID_INVOCATION.getValue());
            return;
        }

        boolean debug = parsedOptions.hasOption(DEBUG_OPTION);
        boolean interactive = parsedOptions.hasOption(INTERACTIVE_OPTION);
        System.out.println("Successfully parsed options!");
        try {
            TreeInfo leftTree = new TreeInfo(left);
            TreeInfo rightTree = new TreeInfo(right);
            System.out.println(leftTree);
            System.out.println(rightTree);
            compareTrees(leftTree, rightTree, byteThreshold, nodeThreshold, debug, interactive);
        } catch (IOException e) {
            System.err.println("Unable to read snapshot: " + e.getMessage());
            ServiceUtils.requestSystemExit(ExitCode.UNEXPECTED_ERROR.getValue());
        }
    }

    private static class TreeInfo {

        private static class TreeNode {

            final String label;
            final long size;
            final List<TreeNode> children = new ArrayList<>();
            long descendantSize;
            long descendantCount;

            TreeNode(String label, long size) {
                this.label = label;
                this.size = size;
            }

            void populateChildren(DataTree dataTree, TreeInfo treeInfo, int depth) {
                DataNode node = dataTree.getNode(label);
                Set<String> childLabels;
                synchronized (node) {
                    childLabels = node.getChildren();
                }
                for (String childName : childLabels) {
                    String childPath = label + "/" + childName;
                    DataNode childNode = dataTree.getNode(childPath);
                    long childSize;
                    synchronized (childNode) {
                        childSize = childNode.data == null ? 0 : childNode.data.length;
                    }
                    TreeNode child = new TreeNode(childPath, childSize);
                    child.populateChildren(dataTree, treeInfo, depth + 1);
                    children.add(child);
                }
                descendantSize = size;
                descendantCount = children.size();
                for (TreeNode child : children) {
                    descendantSize += child.descendantSize;
                    descendantCount += child.descendantCount;
                }
                treeInfo.registerNode(this, depth);
            }

        }

        final TreeNode root;
        long count;
        final List<List<TreeNode>> nodesAtDepths = new ArrayList<>();
        final Map<String, TreeNode> nodesByName = new HashMap<>();

        TreeInfo(File snapshot) throws IOException {
            long beginning = System.nanoTime();
            DataTree dataTree = getSnapshot(snapshot);
            System.out.printf("Deserialized snapshot in %s in %f seconds%n",
                              snapshot.getName(), (System.nanoTime() - beginning) / 1_000_000_000.0);
            beginning = System.nanoTime();
            DataNode rootNode = dataTree.getNode("");
            long size;
            synchronized (rootNode) {
                size = rootNode.data == null ? 0 : rootNode.data.length;
            }
            root = new TreeNode("", size);
            root.populateChildren(dataTree, this, 0);
            System.out.printf("Processed data tree in %f seconds%n", (System.nanoTime() - beginning) / 1_000_000_000.0);
        }

        void registerNode(TreeNode node, int depth) {
            while (depth >= nodesAtDepths.size()) {
                nodesAtDepths.add(new ArrayList<>());
            }
            nodesAtDepths.get(depth).add(node);
            nodesByName.put(node.label, node);
            count++;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append(String.format("Node count: %d%n", count));
            builder.append(String.format("Total size: %d%n", root.descendantSize));
            builder.append(String.format("Max depth: %d%n", nodesAtDepths.size()));
            for (int i = 0; i < nodesAtDepths.size(); i++) {
                builder.append(String.format("Count of nodes at depth %d: %d%n", i, nodesAtDepths.get(i).size()));
            }
            return builder.toString();
        }

    }

    static DataTree getSnapshot(File file) throws IOException {
        return readSnapshot(file).tree;
    }

    static final class SnapshotData {

        final DataTree tree;
        final Map<Long, Integer> sessions;
        final FileHeader header;
        final DataTree.ZxidDigest digest;

        SnapshotData(DataTree tree, Map<Long, Integer> sessions, FileHeader header, DataTree.ZxidDigest digest) {
            this.tree = tree;
            this.sessions = sessions;
            this.header = header;
            this.digest = digest;
        }

    }

    private static final class SnapshotInputArchive extends BinaryInputArchive {

        long nodeRecords;

        SnapshotInputArchive(InputStream input) {
            super(new DataInputStream(input));
        }

        @Override
        public void readRecord(Record record, String tag) throws IOException {
            super.readRecord(record, tag);
            if (record instanceof DataNode) {
                nodeRecords++;
            }
        }

    }

    static SnapshotData readSnapshot(File file) throws IOException {
        try (CheckedInputStream stream = SnapStream.getInputStream(file);
             PushbackInputStream input = new PushbackInputStream(stream)) {
            SnapshotInputArchive archive = new SnapshotInputArchive(input);
            FileHeader header = new FileHeader();
            header.deserialize(archive, "fileheader");
            if (header.getMagic() != FileSnap.SNAP_MAGIC || header.getVersion() != 2) {
                throw new IOException("Unsupported snapshot magic or format version: " + header.getVersion());
            }
            DataTree dataTree = new DataTree();
            Map<Long, Integer> sessions = new HashMap<>();
            SerializeUtils.deserializeSnapshot(dataTree, archive, sessions);
            if (dataTree.getNode("") == null || dataTree.getNode("") != dataTree.getNode("/")) {
                throw new IOException("Invalid snapshot root");
            }
            // DataTree overwrites duplicate paths; count records without a second path index.
            if (archive.nodeRecords != (long) dataTree.getNodeCount() - 1) {
                throw new IOException("Duplicate snapshot node records");
            }
            checkSealIntegrity(stream, archive);

            // Distinguish an absent digest in older snapshots from a truncated digest.
            DataTree.ZxidDigest digest = null;
            int next = input.read();
            if (next != -1) {
                input.unread(next);
                digest = dataTree.new ZxidDigest();
                digest.deserialize(archive);
                checkSealIntegrity(stream, archive);
                if (input.read() != -1) {
                    throw new IOException("Unexpected data after snapshot");
                }
            }
            return new SnapshotData(dataTree, sessions, header, digest);
        } catch (IOException | IllegalArgumentException e) {
            throw new IOException(file + ": " + e, e);
        }
    }

    private static void checkSealIntegrity(CheckedInputStream stream, InputArchive archive) throws IOException {
        // SnapStream's seal checker is package-private on this branch.
        long checksum = stream.getChecksum().getValue();
        long expected = archive.readLong("val");
        String path = archive.readString("path");
        if (checksum != expected || !"/".equals(path)) {
            throw new IOException("CRC corruption or invalid snapshot seal");
        }
    }

    private static void printThresholdInfo(int byteThreshold, int nodeThreshold) {
        System.out.printf("Printing analysis for nodes difference larger than %d bytes or node count difference larger than %d.%n",
                          byteThreshold, nodeThreshold);
    }

    private static void compareTrees(
        TreeInfo left,
        TreeInfo right,
        int byteThreshold,
        int nodeThreshold,
        boolean debug,
        boolean interactive) {
        int maxDepth = Math.max(left.nodesAtDepths.size(), right.nodesAtDepths.size());
        if (!interactive) {
            printThresholdInfo(byteThreshold, nodeThreshold);
            for (int i = 0; i < maxDepth; i++) {
                System.out.printf("Analysis for depth %d%n", i);
                compareLine(left, right, i, byteThreshold, nodeThreshold, debug, false);
            }
        } else {
            try (Scanner scanner = new Scanner(System.in)) {
                int currentDepth = 0;
                while (currentDepth < maxDepth) {
                    System.out.printf("Current depth is %d%n", currentDepth);
                    System.out.println("- Press enter to move to print current depth layer;\n"
                                       + "- Type a number to jump to and print all nodes at a given depth;\n"
                                       + "- Enter an ABSOLUTE path to print the immediate subtree of a node. Path must start with '/'.");
                    if (!scanner.hasNextLine()) {
                        System.out.println("End of input.");
                        return;
                    }
                    String input = scanner.nextLine();
                    printThresholdInfo(byteThreshold, nodeThreshold);
                    if (input.isEmpty()) {
                        System.out.printf("Analysis for depth %d%n", currentDepth);
                        compareLine(left, right, currentDepth, byteThreshold, nodeThreshold, debug, true);
                        currentDepth++;
                    } else if (input.startsWith("/")) {
                        System.out.printf("Analysis for node %s%n", input);
                        compareSubtree(left, right, input, byteThreshold, nodeThreshold, debug);
                    } else {
                        try {
                            int depth = Integer.parseInt(input);
                            if (depth < 0 || depth >= maxDepth) {
                                System.out.printf("Depth must be in range [%d, %d]%n", 0, maxDepth - 1);
                                continue;
                            }
                            currentDepth = depth;
                            System.out.printf("Analysis for depth %d%n", currentDepth);
                            compareLine(left, right, currentDepth, byteThreshold, nodeThreshold, debug, true);
                        } catch (NumberFormatException e) {
                            System.out.printf("Input %s is not valid. Depth must be in range [%d, %d]. "
                                              + "Path must be an absolute path which starts with '/'.%n",
                                              input, 0, maxDepth - 1);
                        }
                    }
                    System.out.println();
                }
            }
        }
        System.out.println("All layers compared.");
    }

    private static void compareSubtree(
        TreeInfo left,
        TreeInfo right,
        String path,
        int byteThreshold,
        int nodeThreshold,
        boolean debug) {
        String label = "/".equals(path) ? "" : path;
        TreeInfo.TreeNode leftRoot = left.nodesByName.get(label);
        TreeInfo.TreeNode rightRoot = right.nodesByName.get(label);
        if (leftRoot == null && rightRoot == null) {
            System.out.printf("Path %s is neither found in left tree nor right tree.%n", path);
        } else {
            List<TreeInfo.TreeNode> leftList = leftRoot == null ? Collections.emptyList() : leftRoot.children;
            List<TreeInfo.TreeNode> rightList = rightRoot == null ? Collections.emptyList() : rightRoot.children;
            compareNodes(leftList, rightList, byteThreshold, nodeThreshold, debug, true);
        }
    }

    private static void compareLine(
        TreeInfo left,
        TreeInfo right,
        int depth,
        int byteThreshold,
        int nodeThreshold,
        boolean debug,
        boolean interactive) {
        List<TreeInfo.TreeNode> leftList = depth >= left.nodesAtDepths.size()
            ? Collections.emptyList() : left.nodesAtDepths.get(depth);
        List<TreeInfo.TreeNode> rightList = depth >= right.nodesAtDepths.size()
            ? Collections.emptyList() : right.nodesAtDepths.get(depth);
        compareNodes(leftList, rightList, byteThreshold, nodeThreshold, debug, interactive);
    }

    private static void compareNodes(
        List<TreeInfo.TreeNode> leftList,
        List<TreeInfo.TreeNode> rightList,
        int byteThreshold,
        int nodeThreshold,
        boolean debug,
        boolean interactive) {
        Comparator<TreeInfo.TreeNode> comparator = Comparator.comparing(node -> node.label);
        Collections.sort(leftList, comparator);
        Collections.sort(rightList, comparator);
        int leftIndex = 0;
        int rightIndex = 0;
        while (leftIndex < leftList.size() || rightIndex < rightList.size()) {
            TreeInfo.TreeNode leftNode = leftIndex < leftList.size() ? leftList.get(leftIndex) : null;
            TreeInfo.TreeNode rightNode = rightIndex < rightList.size() ? rightList.get(rightIndex) : null;
            if (leftNode != null && rightNode != null) {
                if (debug) {
                    System.out.printf("Comparing %s to %s%n", leftNode.label, rightNode.label);
                }
                int result = comparator.compare(leftNode, rightNode);
                if (result < 0) {
                    if (debug) {
                        System.out.println("left is less");
                    }
                    printOnly(leftNode, "left", byteThreshold, nodeThreshold, debug, interactive);
                    leftIndex++;
                } else if (result > 0) {
                    if (debug) {
                        System.out.println("right is less");
                    }
                    printOnly(rightNode, "right", byteThreshold, nodeThreshold, debug, interactive);
                    rightIndex++;
                } else {
                    if (debug) {
                        System.out.println("same");
                    }
                    printBoth(leftNode, rightNode, byteThreshold, nodeThreshold, debug, interactive);
                    leftIndex++;
                    rightIndex++;
                }
            } else if (leftNode != null) {
                printOnly(leftNode, "left", byteThreshold, nodeThreshold, debug, interactive);
                leftIndex++;
            } else {
                printOnly(rightNode, "right", byteThreshold, nodeThreshold, debug, interactive);
                rightIndex++;
            }
        }
    }

    private static void printOnly(
        TreeInfo.TreeNode node,
        String side,
        int byteThreshold,
        int nodeThreshold,
        boolean debug,
        boolean interactive) {
        if (node.descendantSize > byteThreshold || node.descendantCount > nodeThreshold) {
            System.out.printf("Node %s found only in %s tree. Descendant size: %d. Descendant count: %d%n",
                              node.label, side, node.descendantSize, node.descendantCount);
        } else if (debug || interactive) {
            System.out.printf("Filtered %s node %s of size %d%n", side, node.label, node.descendantSize);
        }
    }

    private static void printBoth(
        TreeInfo.TreeNode left,
        TreeInfo.TreeNode right,
        int byteThreshold,
        int nodeThreshold,
        boolean debug,
        boolean interactive) {
        long byteDelta = right.descendantSize - left.descendantSize;
        long nodeDelta = right.descendantCount - left.descendantCount;
        if (Math.abs(byteDelta) > byteThreshold || Math.abs(nodeDelta) > nodeThreshold) {
            System.out.printf("Node %s found in both trees. Delta: %d bytes, %d descendants%n",
                              left.label, byteDelta, nodeDelta);
        } else if (debug || interactive) {
            System.out.printf("Filtered node %s of left size %d, right size %d%n",
                              left.label, left.descendantSize, right.descendantSize);
        }
    }

}
