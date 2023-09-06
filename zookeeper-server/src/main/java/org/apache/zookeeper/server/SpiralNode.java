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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.data.StatPersisted;

/**
 * This class contains the data for a node in the data tree.
 * <p>
 * A data node contains a reference to its parent, a byte array as its data, an
 * array of ACLs, a stat object, and a set of its children's paths.
 *
 */
@SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class SpiralNode implements Record {

    /** the data for this datanode */
    public byte[] data;

    /**
     * the acl map long for this datanode. the datatree has the map
     */
    public Long acl;

    /**
     * the stat for this node that is persisted to disk.
     */
    public StatPersisted stat;

    int numberOfChildren = 0;

    SpiralNode() {
        // default constructor
    }

    /**
     * create a Spiral with data, acls and stat
     */
    public SpiralNode(byte[] data, Long acl, StatPersisted stat) {
        this.data = data;
        this.acl = acl;
        this.stat = stat;
    }

    public synchronized void deserialize(InputArchive archive, String tag) throws IOException {
        archive.startRecord("node");
        data = archive.readBuffer("data");
        acl = archive.readLong("acl");
        stat = new StatPersisted();
        stat.deserialize(archive, "statpersisted");
        archive.endRecord("node");
    }

    public synchronized void serialize(OutputArchive archive, String tag) throws IOException {
        archive.startRecord(this, tag);
        archive.writeBuffer(data, "data");
        archive.writeLong(acl, "acl");
        stat.serialize(archive, "statpersisted");
        archive.endRecord(this, tag);
    }

    public byte[] toByteBuffer() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryOutputArchive archive = BinaryOutputArchive.getArchive(baos);
        serialize(archive, "node");
        return baos.toByteArray();
    }

    public static Stat convert2Stat(StatPersisted statPersisted) {
        Stat stat = new Stat();
        stat.setAversion(statPersisted.getAversion());
        stat.setCtime(statPersisted.getCtime());
        stat.setCversion(statPersisted.getCversion());
        stat.setCzxid(statPersisted.getCzxid());
        stat.setMtime(statPersisted.getMtime());
        stat.setMzxid(statPersisted.getMzxid());
        stat.setPzxid(statPersisted.getPzxid());
        stat.setVersion(statPersisted.getVersion());
        stat.setEphemeralOwner(statPersisted.getEphemeralOwner());
        stat.setDataLength(0);
        stat.setNumChildren(0);
        return stat;
    }
}
