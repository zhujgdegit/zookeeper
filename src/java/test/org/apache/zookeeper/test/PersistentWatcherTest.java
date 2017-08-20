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

package org.apache.zookeeper.test;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class PersistentWatcherTest extends ClientBase {
    private static final Logger LOG = LoggerFactory.getLogger(PersistentWatcherTest.class);
    private BlockingQueue<WatchedEvent> events;
    private Watcher persistentWatcher;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        events = new LinkedBlockingQueue<>();
        persistentWatcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                events.add(event);
            }
        };
    }

    @Test
    public void testBasic()
            throws IOException, InterruptedException, KeeperException {
        try ( ZooKeeper zk = createClient(new CountdownWatcher(), hostPort) ) {
            zk.addPersistentWatch("/a/b", persistentWatcher, false);
            internalTestBasic(zk);
        }
    }

    @Test
    public void testBasicAsync()
            throws IOException, InterruptedException, KeeperException {
        try ( ZooKeeper zk = createClient(new CountdownWatcher(), hostPort) ) {
            final CountDownLatch latch = new CountDownLatch(1);
            AsyncCallback.VoidCallback cb = new AsyncCallback.VoidCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx) {
                    if (rc == 0) {
                        latch.countDown();
                    }
                }
            };
            zk.addPersistentWatch("/a/b", persistentWatcher, false, cb, null);
            Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
            internalTestBasic(zk);
        }
    }

    private void internalTestBasic(ZooKeeper zk) throws KeeperException, InterruptedException {
        zk.create("/a", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/a/b", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/a/b/c", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.setData("/a/b", new byte[0], -1);
        zk.delete("/a/b/c", -1);
        zk.delete("/a/b", -1);
        zk.create("/a/b", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        assertEvent(events, Watcher.Event.EventType.NodeCreated, "/a/b");
        assertEvent(events, Watcher.Event.EventType.NodeChildrenChanged, "/a/b");
        assertEvent(events, Watcher.Event.EventType.NodeDataChanged, "/a/b");
        assertEvent(events, Watcher.Event.EventType.NodeChildrenChanged, "/a/b");
        assertEvent(events, Watcher.Event.EventType.NodeDeleted, "/a/b");
        assertEvent(events, Watcher.Event.EventType.NodeCreated, "/a/b");
    }

    @Test
    public void testRemoval()
            throws IOException, InterruptedException, KeeperException {
        try ( ZooKeeper zk = createClient(new CountdownWatcher(), hostPort) ) {
            zk.addPersistentWatch("/a/b", persistentWatcher, false);
            zk.create("/a", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/a/b", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/a/b/c", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            assertEvent(events, Watcher.Event.EventType.NodeCreated, "/a/b");
            assertEvent(events, Watcher.Event.EventType.NodeChildrenChanged, "/a/b");

            zk.removeWatches("/a/b", persistentWatcher, Watcher.WatcherType.Any, false);
            zk.delete("/a/b/c", -1);
            zk.delete("/a/b", -1);
            assertEvent(events, Watcher.Event.EventType.PersistentWatchRemoved, "/a/b");
        }
    }

    @Test
    public void testDisconnect() throws Exception {
        try ( ZooKeeper zk = createClient(new CountdownWatcher(), hostPort) ) {
            zk.addPersistentWatch("/a/b", persistentWatcher, false);
            stopServer();
            assertEvent(events, Watcher.Event.EventType.None, null);
            startServer();
            assertEvent(events, Watcher.Event.EventType.None, null);
            internalTestBasic(zk);
        }
    }

    @Test
    public void testMultiClient()
            throws IOException, InterruptedException, KeeperException {
        ZooKeeper zk1 = null;
        ZooKeeper zk2 = null;
        try {
            zk1 = createClient(new CountdownWatcher(), hostPort);
            zk2 = createClient(new CountdownWatcher(), hostPort);

            zk1.create("/a", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk1.create("/a/b", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

            zk1.addPersistentWatch("/a/b", persistentWatcher, false);
            zk1.setData("/a/b", "one".getBytes(), -1);
            Thread.sleep(1000); // give some time for the event to arrive

            zk2.setData("/a/b", "two".getBytes(), -1);
            zk2.setData("/a/b", "three".getBytes(), -1);
            zk2.setData("/a/b", "four".getBytes(), -1);

            assertEvent(events, Watcher.Event.EventType.NodeDataChanged, "/a/b");
            assertEvent(events, Watcher.Event.EventType.NodeDataChanged, "/a/b");
            assertEvent(events, Watcher.Event.EventType.NodeDataChanged, "/a/b");
            assertEvent(events, Watcher.Event.EventType.NodeDataChanged, "/a/b");
        } finally {
            if (zk1 != null) {
                zk1.close();
            }
            if (zk2 != null) {
                zk2.close();
            }
        }
    }

    @Test
    public void testRootWatcher()
            throws IOException, InterruptedException, KeeperException {
        try ( ZooKeeper zk = createClient(new CountdownWatcher(), hostPort) ) {
            zk.addPersistentWatch("/", persistentWatcher, false);
            zk.create("/a", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.setData("/a", new byte[0], -1);
            zk.create("/b", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            assertEvent(events, Watcher.Event.EventType.NodeChildrenChanged, "/");
            assertEvent(events, Watcher.Event.EventType.NodeChildrenChanged, "/");
        }
    }

    private void assertEvent(BlockingQueue<WatchedEvent> events, Watcher.Event.EventType eventType, String path)
            throws InterruptedException {
        WatchedEvent event = events.poll(5, TimeUnit.SECONDS);
        Assert.assertNotNull(event);
        Assert.assertEquals(eventType, event.getType());
        Assert.assertEquals(path, event.getPath());
    }
}
