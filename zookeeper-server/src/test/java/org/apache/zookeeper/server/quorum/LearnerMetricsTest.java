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

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.metrics.MetricsUtils;
import org.apache.zookeeper.server.ServerMetrics;
import org.apache.zookeeper.test.ClientBase;
import org.hamcrest.Matcher;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo;

public class LearnerMetricsTest extends QuorumPeerTestBase {

    private static final int TIMEOUT_SECONDS = 30;

    @Test
    public void testLearnerMetricsTest() throws Exception {
        ServerMetrics.getMetrics().resetAll();
        ClientBase.setupTestEnv();

        final int SERVER_COUNT = 6; // 5 participants, 1 observer
        final String path = "/zk-testLeanerMetrics";
        final byte[] data = new byte[512];
        final int[] clientPorts = new int[SERVER_COUNT];
        StringBuilder sb = new StringBuilder();
        int observer = 0 ;
        clientPorts[observer] = PortAssignment.unique();
        sb.append("server."+observer+"=127.0.0.1:"+PortAssignment.unique()+":"+PortAssignment.unique()+":observer\n");
        for(int i = 1; i < SERVER_COUNT; i++) {
            clientPorts[i] = PortAssignment.unique();
            sb.append("server."+i+"=127.0.0.1:"+PortAssignment.unique()+":"+PortAssignment.unique()+"\n");
        }

        // start the participants
        String quorumCfgSection = sb.toString();
        QuorumPeerTestBase.MainThread[] mt = new QuorumPeerTestBase.MainThread[SERVER_COUNT];
        for(int i = 1; i < SERVER_COUNT; i++) {
            mt[i] = new QuorumPeerTestBase.MainThread(i, clientPorts[i], quorumCfgSection);
            mt[i].start();
        }

        // start the observer
        Map<String, String> observerConfig = new HashMap<>();
        observerConfig.put("peerType", "observer");
        mt[observer] = new QuorumPeerTestBase.MainThread(observer, clientPorts[observer], quorumCfgSection, observerConfig);
        mt[observer].start();

        // send one create request
        ZooKeeper zk = new ZooKeeper("127.0.0.1:" + clientPorts[1], ClientBase.CONNECTION_TIMEOUT, this);
        waitForOne(zk, ZooKeeper.States.CONNECTED);
        zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        // there are 4 followers, each received two proposals, one for leader election, one for the create request
        waitForMetric("learner_proposal_received_count", is(8L));
        waitForMetric("cnt_proposal_latency", is(8L));
        waitForMetric("min_proposal_latency", greaterThanOrEqualTo(0L));
        waitForMetric("cnt_proposal_ack_creation_latency", is(10L));
        waitForMetric("min_proposal_ack_creation_latency", greaterThanOrEqualTo(0L));

        // there are five learners, each received two commits, one for leader election, one for the create request
        waitForMetric("learner_commit_received_count", is(10L));
        waitForMetric("cnt_commit_propagation_latency", is(10L));
        waitForMetric("min_commit_propagation_latency", greaterThanOrEqualTo(0L));
    }

    private void waitForMetric(final String metricKey, final Matcher<Long> matcher) throws InterruptedException {
        final String errorMessage = String.format("unable to match on metric: %s", metricKey);
        waitFor(errorMessage,
                () -> {
                    long actual = (long) MetricsUtils.currentServerMetrics().get(metricKey);
                    if(!matcher.matches(actual)) {
                        LOG.info(String.format("match failed on %s, actual value: %d", metricKey, actual));
                        return false;
                    }
                    return true;
                },
                TIMEOUT_SECONDS);
    }
}
