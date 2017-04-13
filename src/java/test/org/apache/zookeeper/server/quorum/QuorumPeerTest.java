/**
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

package org.apache.zookeeper.server.quorum;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.common.SocketAddressUtils;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.test.ClientBase;
import org.junit.Test;

public class QuorumPeerTest {

    private int electionAlg = 3;
    private int tickTime = 2000;
    private int initLimit = 3;
    private int syncLimit = 3;

    /**
     * Test case for https://issues.apache.org/jira/browse/ZOOKEEPER-2301
     */
    @Test
    public void testQuorumPeerListendOnSpecifiedClientIP() throws IOException {
        long myId = 1;
        File dataDir = ClientBase.createTmpDir();
        int clientPort = PortAssignment.unique();
        Map<Long, QuorumServer> peersView = new HashMap<Long, QuorumServer>();
        InetAddress clientIP = InetAddress.getLoopbackAddress();

        peersView.put(Long.valueOf(myId),
                new QuorumServer(myId, new InetSocketAddress(clientIP, PortAssignment.unique()),
                        new InetSocketAddress(clientIP, PortAssignment.unique()),
                        new InetSocketAddress(clientIP, clientPort), LearnerType.PARTICIPANT));

        /**
         * QuorumPeer constructor without QuorumVerifier
         */
        QuorumPeer peer1 = new QuorumPeer(peersView, dataDir, dataDir, clientPort, electionAlg, myId, tickTime,
                initLimit, syncLimit);
        String hostString1 = SocketAddressUtils.getHostString(peer1.cnxnFactory.getLocalAddress());
        assertEquals(clientIP.getHostAddress(), hostString1);

        // cleanup
        peer1.shutdown();

        /**
         * QuorumPeer constructor with QuorumVerifier
         */
        peersView.clear();
        clientPort = PortAssignment.unique();
        peersView.put(Long.valueOf(myId),
                new QuorumServer(myId, new InetSocketAddress(clientIP, PortAssignment.unique()),
                        new InetSocketAddress(clientIP, PortAssignment.unique()),
                        new InetSocketAddress(clientIP, clientPort), LearnerType.PARTICIPANT));
        QuorumPeer peer2 = new QuorumPeer(peersView, dataDir, dataDir, clientPort, electionAlg, myId, tickTime,
                initLimit, syncLimit);
        String hostString2 = SocketAddressUtils.getHostString(peer2.cnxnFactory.getLocalAddress());
        assertEquals(clientIP.getHostAddress(), hostString2);
        // cleanup
        peer2.shutdown();
    }

}
