/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.watch;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.zookeeper.AddWatchMode.PERSISTENT;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A benchmark tool that benchmarks the watch throughput and latency.
 * See ZOOKEEPER-3823 for the design document
 */

public class WatchBenchmarkTool {
    private static final Logger LOG = LoggerFactory.getLogger(WatchBenchmarkTool.class);

    private static final ConcurrentHashMap<Integer, Long> watchTriggerTimeMap = new ConcurrentHashMap<>();
    private static final List<Long> latencyList = new Vector<>();
    private static AtomicLong totalStartTriggerWatchTime = new AtomicLong(0);
    private static int timeout;
    private static boolean isDebug;

    private static String rootPath;
    private static int znodeCount;
    private static int znodeSize = 1;
    private static int clientThreads;
    private static String connectString;
    private static int sessionTimeout;
    private static String configFilePath;
    private static String watchMode;
    private static int watchMultiple = 1;

    public static void main(String[] args) throws Exception {
        long totalStartTime = System.currentTimeMillis();
        Options options = new Options();
        options.addOption("connect_string", true, "ZooKeeper connectString. Default: 127.0.0.1:2181");
        options.addOption("root_path", true, "Root Path for creating znodes for the benchmark. Not empty");
        options.addOption("znode_count", true, "The znode count. Default: 1000");
        options.addOption("znode_size", true, "The data length of per znode. Default: 1");
        options.addOption("threads", true, "The client thread number. Default: 1");
        options.addOption("session_timeout", true, "ZooKeeper sessionTimeout. Default: 40000 ms");
        options.addOption("force", false, "Force to run the benchmark, even if root_path exists");
        options.addOption("timeout", true, "Timeout for waiting for all watch events arrival. Default: 10000 ms");
        options.addOption("client_configuration", true, "Client configuration file to set some special client setting. Default: empty");
        options.addOption("watch_mode", true, "Watch mode. Optional value is t or p, corresponding to traditional one-off watch or persistent watch. Default: t");
        options.addOption("watch_multiple", true, "Watch multiple times when enables persistent watch. Default: 1");
        options.addOption("v", false, "Verbose output, print some logs for debugging");
        options.addOption("help", false, "Help message");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);

        if (args.length == 0 || cmd.hasOption("help")) {
            usage(options);
            System.exit(-1);
        }

        checkParameters(cmd);

        // submit tasks to thread pool
        ExecutorService executorService = Executors.newFixedThreadPool(clientThreads);
        CyclicBarrier createNodeCyclicBarrier = new CyclicBarrier(clientThreads);
        CyclicBarrier setWatchCyclicBarrier = new CyclicBarrier(clientThreads);
        CountDownLatch deleteNodeCountDownLatch = new CountDownLatch(clientThreads);
        CountDownLatch finishWatchCountDownLatch = new CountDownLatch(watchMultiple * clientThreads * znodeCount);
        CountDownLatch closeClientCountDownLatch = new CountDownLatch(1);
        AtomicBoolean syncOnce = new AtomicBoolean(false);
        for (int i = 0; i < clientThreads; i++) {
            executorService.execute(new WatchClientThread(i, createNodeCyclicBarrier,
                    setWatchCyclicBarrier, deleteNodeCountDownLatch, finishWatchCountDownLatch, closeClientCountDownLatch, syncOnce));
        }

        // wait for deleting all nodes
        long deleteAwaitStart = System.currentTimeMillis();
        deleteNodeCountDownLatch.await();
        if (isDebug) {
            LOG.info("deleteNodeCountDownLatch await time spent: {} ms", (System.currentTimeMillis() - deleteAwaitStart));
        }

        /** wait for all watch events arrival, especially network latency or overhead workloads
         *  In most cases, when znodes have been deleted, most of the watch events has been notified
         */
        long finishWatchAwaitStart = System.currentTimeMillis();
        boolean finishAwaitFlag = finishWatchCountDownLatch.await(timeout, TimeUnit.MILLISECONDS);
        if (isDebug) {
            LOG.info("finishWatchCountDownLatch await time spent: {} ms, awaitFlag:{}", (System.currentTimeMillis() - finishWatchAwaitStart), finishAwaitFlag);
        }
        long latencyListSnapshotSize = latencyList.size();
        long endTime = System.currentTimeMillis();
        long totalWatchSpentTime = endTime - totalStartTriggerWatchTime.longValue();
        if (isDebug) {
            LOG.info("totalStartTriggerWatchTime: {}, endTime: {}, totalWatchSpentTime: {} ms ", totalStartTriggerWatchTime, endTime, totalWatchSpentTime);
        }

        // close all the zk clients
        closeClientCountDownLatch.countDown();
        // shutdown thread pool
        shutDownThreadPool(executorService);
        // show the summary
        showBenchmarkReport(totalStartTime, totalWatchSpentTime, latencyListSnapshotSize);
    }

    private static void showBenchmarkReport(long totalStartTime, long totalWatchSpentTime, long latencyListSnapshotSize) {
        if (latencyListSnapshotSize == 0) {
            System.out.println("Latency list is empty, cannot show the benchmark report");
            return;
        }

        /**
         * A deep copy of the latencyList, to avoid this situation when we statistics latencyList
         * at the same time, watch events in flight are added to latencyList concurrently.
         */
        List<Long> copyLatencyList = new LinkedList<>();
        copyLatencyList.addAll(latencyList);
        // Now, we can clear the latencyList to save the memory
        latencyList.clear();

        // receive, loss notifications count and ratio summary
        double receivedRatio = (double) copyLatencyList.size() / (double) (watchMultiple * clientThreads * znodeCount);
        long lossCount = watchMultiple * clientThreads * znodeCount - copyLatencyList.size();
        double lossRatio = (double) lossCount / (double) (watchMultiple * clientThreads * znodeCount);
        System.out.println();
        System.out.println("Notification expected count: " + (watchMultiple * clientThreads * znodeCount)
                + ", received count: " + copyLatencyList.size() + " (" + getFormatedDouble(receivedRatio) + ")"
                + ", loss count: " + lossCount + " (" + getFormatedDouble(lossRatio) + ")");

        // latency distribution
        printLatencyDistribution(copyLatencyList);

        // throughput
        double timeInμsPerNotification = (double) (totalWatchSpentTime * 1000) / (double) latencyListSnapshotSize;
        if (isDebug) {
            LOG.info("timeInμsPerNotification: {} μs, latencyListSnapshotSize:{}, latencyList.size():{}", timeInμsPerNotification, latencyListSnapshotSize, latencyList.size());
        }
        System.out.println("Total time:" + (System.currentTimeMillis() - totalStartTime) + " ms, watch benchmark total time: "
                + totalWatchSpentTime + " ms, throughput:" + getFormatedDouble(1000 * 1000 / timeInμsPerNotification) + " op/s");

    }

    private static void shutDownThreadPool(ExecutorService executorService) {
        long shutDownStart = System.currentTimeMillis();
        executorService.shutdown();
        while (true) {
            try {
                if (executorService.isTerminated()) {
                    break;
                }
                Thread.sleep(200);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (isDebug) {
            LOG.info("Shutdown all the WatchClientThread in {} ms ", (System.currentTimeMillis() - shutDownStart));
        }
    }

    private static void usage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("WatchBenchmarkTool <options>", options);
    }

    private static void checkParameters(CommandLine cmd) {
        // root_path
        rootPath = cmd.getOptionValue("root_path");
        PathUtils.validatePath(rootPath);
        if ("/".equals(rootPath)) {
            throw new IllegalArgumentException("root_path must not be set with '/'");
        }

        sessionTimeout = Integer.parseInt(cmd.getOptionValue("session_timeout", "40000"));
        checkOptionNumber("session_timeout", sessionTimeout, 0);
        connectString = cmd.getOptionValue("connect_string", "127.0.0.1:2181");
        configFilePath = cmd.getOptionValue("client_configuration");
        // znodes
        znodeCount = Integer.parseInt(cmd.getOptionValue("znode_count", "1000"));
        checkOptionNumber("znode_count", znodeCount, 0);
        // znode_size
        znodeSize = Integer.parseInt(cmd.getOptionValue("znode_size", "1"));
        checkOptionNumber("znode_size", znodeSize, 0);
        // threads
        clientThreads = Integer.parseInt(cmd.getOptionValue("threads", "1"));
        checkOptionNumber("threads", clientThreads, 0);
        if (clientThreads > 60) {
            LOG.warn("The clientThreads set {} has exceeded the default maxClientCnxns value:60. Note you should also set this property in the server side", clientThreads);
        }
        timeout = Integer.parseInt(cmd.getOptionValue("timeout", "10000"));
        checkOptionNumber("timeout", timeout, 0);
        isDebug = cmd.hasOption("v");
        // watch_mode
        watchMode = cmd.getOptionValue("watch_mode", "t");
        if (watchMode.equals(WatchMode.TRADITION.getAbbreviation()) && cmd.hasOption("watch_multiple")) {
            throw new IllegalArgumentException("watch_multiple must not be set in the traditional one-off watch mode");
        }
        if (WatchMode.getValue(watchMode) == null) {
            throw new IllegalArgumentException("don't support this watch mode option: " + watchMode);
        }
        // watch_multiple
        watchMultiple = Integer.parseInt(cmd.getOptionValue("watch_multiple", "1"));
        checkOptionNumber("watch_multiple", watchMultiple, 0);
        if (watchMode.equals(WatchMode.PERSISTENT.getAbbreviation())) {
            LOG.warn("The PERSISTENT watch is available since 3.6.0, please make sure the release version of ZooKeeper server");
        }

        createWorkSpace(cmd);
    }

    private static void checkOptionNumber(String optionName, int optionVal, int threshold) {
        if (optionVal <= threshold) {
            throw new IllegalArgumentException(optionName + " must be greater than " + threshold);
        }
    }

    private static void createWorkSpace(CommandLine cmd) {
        try {
            ZooKeeper zk = initZKClient();
            if (zk.exists(rootPath, null) != null) {
                if (!cmd.hasOption("force")) {
                    throw new IllegalArgumentException("cannot test under the existing rootPath:" + rootPath + " without force option");
                }
            } else {
                // help user to create the znode: rootPath
                String[] paths = rootPath.split("/");
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i < paths.length; i++) {
                    sb.append("/" + paths[i]);
                    try {
                        zk.create(sb.toString(), "".getBytes(UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                CreateMode.PERSISTENT);
                    } catch (KeeperException.NodeExistsException e) {
                        // ignore it
                    }
                }
            }
        } catch (IOException | InterruptedException | KeeperException | QuorumPeerConfig.ConfigException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static ZooKeeper initZKClient() throws IOException, QuorumPeerConfig.ConfigException {
        ZooKeeper zk;
        if (StringUtils.isBlank(configFilePath)) {
            zk = new ZooKeeper(connectString, sessionTimeout, null);
        } else {
            ZKClientConfig config = new ZKClientConfig(configFilePath);
            zk = new ZooKeeper(connectString, sessionTimeout, null, false, config);
        }
        return zk;
    }

    private static void printLatencyDistribution(List<Long> copyLatencyList) {
        long start = System.currentTimeMillis();
        Collections.sort(copyLatencyList);
        long end = System.currentTimeMillis();
        if (isDebug) {
            LOG.info("sort the latencyList(size: {}) time spent: {} ms", copyLatencyList.size(), (end - start));
        }

        /**
         * filter out the invalid watch latency which we cannot figure out its start trigger watch time when setting watch multiply
         * times in PERSISTENT watch mode
         */
        filterLatencyList(copyLatencyList);
        if (isDebug) {
            LOG.info("filter out LatencyList(size: {}) time spent: {} ms", copyLatencyList.size(), (System.currentTimeMillis() - end));
        }
        if (copyLatencyList.size() == 0) {
            System.out.println("Latency list is empty, cannot print latency distribution");
            return;
        }
        System.out.println("[Latency distribution]: ");
        System.out.println("Avg latency: " + getFormatedDouble(getAvgLatency(copyLatencyList)) + " ms");
        System.out.println("Fastest latency: " + getFormatedDouble(getFastestLatency(copyLatencyList)) + " ms");
        System.out.println("Slowest latency: " + getFormatedDouble(getSlowestLatency(copyLatencyList)) + " ms");

        List<Integer> percentileList = new ArrayList<>();
        percentileList.add(10);
        percentileList.add(25);
        percentileList.add(50);
        percentileList.add(75);
        percentileList.add(90);
        percentileList.add(95);
        percentileList.add(99);
        List<Long> resultList = percentile(copyLatencyList, percentileList);
        if (percentileList.size() != resultList.size()) {
            LOG.info("percentileList.size():{} is not equal to resultList.size():{}", percentileList.size(), resultList.size());
            return;
        }
        for (int i = 0; i < percentileList.size(); i++) {
            Integer percentile = percentileList.get(i);
            System.out.println(percentile + "th percentile notification latency: " + resultList.get(i) + " ms");
        }
    }

    private static void filterLatencyList(List<Long> copyLatencyList) {
        Iterator<Long> iterator = copyLatencyList.iterator();
        while (iterator.hasNext()) {
            Long value = iterator.next();
            if (value <= 0) {
                iterator.remove();
            } else {
                break;
            }
        }
    }

    private static double getSlowestLatency(List<Long> latencyList) {
        if (latencyList.size() == 0) {
            return -1;
        }
        return latencyList.get(latencyList.size() - 1);
    }

    private static double getFastestLatency(List<Long> latencyList) {
        if (latencyList.size() == 0) {
            return -1;
        }
        return (double) latencyList.get(0);
    }

    private static double getAvgLatency(List<Long> latencyList) {
        if (latencyList.size() == 0) {
            return -1;
        }
        long total = 0;
        for (Long latency : latencyList) {
            total += latency;
        }
        return (double) total / (double) latencyList.size();
    }

    private static double getFormatedDouble(double value) {
        BigDecimal bg = new BigDecimal(value);
        return bg.setScale(4, RoundingMode.HALF_UP).doubleValue();
    }

    private static List<Long> percentile(List<Long> latency, List<Integer> percentiles) {
        int size = latency.size();
        List<Long> resultList = new ArrayList<>();
        if (size == 0) {
            return resultList;
        }
        for (Integer percentile : percentiles) {
            double percent = (double) percentile / 100;
            int sampleSize = (int) (size * percent);
            resultList.add(latency.get(sampleSize - 1 < 0 ? 0 : sampleSize - 1));
        }
        return resultList;
    }

    /**
     * WatchClientThread does the following things:
     *     create corresponding znodes if needed(when threads > znodes, some threads don't do this operation)
     *     set watch for all znodes
     *     trigger watch by issuing write requests
     *     delete corresponding znodes if needed(when threads > znodes, some threads don't do this operation)
     *     close zk client
     */
    static class WatchClientThread implements Runnable {
        private Integer threadIndex;
        private CyclicBarrier createNodeCyclicBarrier;
        private CyclicBarrier setWatchCyclicBarrier;
        private CountDownLatch deleteNodeCountDownLatch;
        private CountDownLatch finishWatchCountDownLatch;
        private CountDownLatch closeClientCountDownLatch;
        private AtomicBoolean syncOnce;

        public WatchClientThread(Integer threadIndex, CyclicBarrier createNodeCyclicBarrier,
                                 CyclicBarrier setWatchCyclicBarrier, CountDownLatch deleteNodeCountDownLatch,
                                 CountDownLatch finishWatchCountDownLatch, CountDownLatch closeClientCountDownLatch, AtomicBoolean syncOnce) {
            this.threadIndex = threadIndex;
            this.createNodeCyclicBarrier = createNodeCyclicBarrier;
            this.setWatchCyclicBarrier = setWatchCyclicBarrier;
            this.deleteNodeCountDownLatch = deleteNodeCountDownLatch;
            this.finishWatchCountDownLatch = finishWatchCountDownLatch;
            this.closeClientCountDownLatch = closeClientCountDownLatch;
            this.syncOnce = syncOnce;
        }

        @Override
        public void run() {
            ZooKeeper zk = null;
            try {
                zk = initZKClient();

                // create
                createNode(zk);
                // block here waiting for all the threads creating its corresponding znodes, then go ahead together
                createNodeCyclicBarrier.await();
                if (isDebug) {
                    LOG.info("WatchClientThread (threadIndex:{}) has finished creating its corresponding znodes", threadIndex);
                }

                // set watch
                SimpleWatcher simpleWatcher = new SimpleWatcher(finishWatchCountDownLatch);
                setWatchForAllNodes(zk, simpleWatcher);
                // block here waiting for all the threads setting watch for all znodes, then go ahead together
                setWatchCyclicBarrier.await();
                if (isDebug) {
                    LOG.info("WatchClientThread (threadIndex:{}) has finished setting watch for all znodes", threadIndex);
                }

                // make sure only one thread(the fastest one) enters this code to record/assign the total start trigger Watch Time
                if (syncOnce.compareAndSet(false, true)) {
                    totalStartTriggerWatchTime = new AtomicLong(System.currentTimeMillis());
                }

                // start to trigger watch by issuing write requests
                // setData
                setNode(zk);
                // delete, also as a function to clean up the workspace
                deleteNode(zk);
                deleteNodeCountDownLatch.countDown();
                if (isDebug) {
                    LOG.info("WatchClientThread (threadIndex:{}) has finished deleting its corresponding znodes", threadIndex);
                }
            } catch (InterruptedException | BrokenBarrierException | IOException | QuorumPeerConfig.ConfigException e) {
                LOG.warn("WatchClientThread (threadIndex:{}) encounters exception", threadIndex, e);
            } finally {
                if (zk != null) {
                    try {
                        if (isDebug) {
                            LOG.info("WatchClientThread (threadIndex:{}) has started to close the zk client", threadIndex);
                        }
                        closeClientCountDownLatch.await();
                        zk.close();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            if (isDebug) {
                LOG.info("WatchClientThread (threadIndex:{}) has finished its task and exited", threadIndex);
            }
        }

        private void createNode(ZooKeeper zk) {
            for (int i = 0; (i * clientThreads + threadIndex) < znodeCount; i++) {
                int path = (i * clientThreads + threadIndex);
                try {
                    String data = RandomStringUtils.randomAlphanumeric(znodeSize);
                    zk.create(rootPath + "/" + path, data.getBytes(UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        private void setWatchForAllNodes(ZooKeeper zk, SimpleWatcher simpleWatcher) {
            for (int i = 0; i < znodeCount; i++) {
                try {
                    if (watchMode.equals(WatchMode.TRADITION.getAbbreviation())) {
                        zk.exists(rootPath + "/" + i, simpleWatcher);
                    } else {
                        zk.addWatch(rootPath + "/" + i, simpleWatcher, PERSISTENT);
                    }
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        private void deleteNode(ZooKeeper zk) {
            watchTriggerTimeMap.clear();
            for (int i = 0; (i * clientThreads + threadIndex) < znodeCount; i++) {
                int path = (i * clientThreads + threadIndex);
                watchTriggerTimeMap.put(path, System.currentTimeMillis());
                try {
                    zk.delete(rootPath + "/" + path, -1);
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        private void setNode(ZooKeeper zk) {
            for (int i = 0; i < watchMultiple - 1; i++) {
                watchTriggerTimeMap.clear();
                setNodeOnce(zk);
            }
        }

        private void setNodeOnce(ZooKeeper zk) {
            for (int i = 0; (i * clientThreads + threadIndex) < znodeCount; i++) {
                int path = (i * clientThreads + threadIndex);
                watchTriggerTimeMap.put(path, System.currentTimeMillis());
                try {
                    String data = RandomStringUtils.randomAlphanumeric(znodeSize);
                    zk.setData(rootPath + "/" + path, data.getBytes(), -1);
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static class SimpleWatcher implements Watcher {
        private CountDownLatch finishWatchCountDownLatch;

        public SimpleWatcher(CountDownLatch finishWatchCountDownLatch) {
            this.finishWatchCountDownLatch = finishWatchCountDownLatch;
        }

        public void process(WatchedEvent e) {
            try {
                if (e.getType() == Event.EventType.None) {
                    return;
                }
                if (e.getState() == Event.KeeperState.SyncConnected) {
                    String pathIndex = e.getPath().substring(e.getPath().lastIndexOf("/") + 1);
                    Long startTriggerTime = watchTriggerTimeMap.get(Integer.parseInt(pathIndex));
                    if (startTriggerTime != null) {
                        latencyList.add(System.currentTimeMillis() - startTriggerTime);
                    } else {
                        latencyList.add(-1L);
                    }
                    finishWatchCountDownLatch.countDown();
                    if (isDebug) {
                        LOG.info("finishWatchCountDownLatch.getCount(): {}, pathIndex: {}", finishWatchCountDownLatch.getCount()
                                , pathIndex);
                    }
                }
            } catch (Exception ex) {
                LOG.warn("SimpleWatcher process watch path:{}, exception", e.getPath(), ex);
            }
        }
    }

    enum WatchMode {
        // the traditional one-off watch
        TRADITION("traditional_watch", "t"),
        PERSISTENT("persistent_watch", "p");
        private String name;
        private String abbreviation;

        WatchMode(String name, String abbreviation) {
            this.name = name;
            this.abbreviation = abbreviation;
        }

        public static WatchMode getValue(String value) {
            for (WatchMode mode : WatchMode.values()) {
                if (mode.getAbbreviation().equals(value)) {
                    return mode;
                }
            }
            return null;
        }

        private String getAbbreviation() {
            return abbreviation;
        }
    }
}