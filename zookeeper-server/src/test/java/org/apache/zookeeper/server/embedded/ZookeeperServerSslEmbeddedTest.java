package org.apache.zookeeper.server.embedded;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing permissions and limitations under the License.
 */

import org.apache.zookeeper.util.PortManager;
import static org.junit.Assert.assertTrue;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.test.ClientBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class ZookeeperServerSslEmbeddedTest {

    @BeforeAll
    public static void setUpEnvironment() {
        System.setProperty("zookeeper.admin.enableServer", "false");
        System.setProperty("zookeeper.4lw.commands.whitelist", "*");
    }

    @AfterAll
    public static void cleanUpEnvironment() throws InterruptedException, IOException {
        System.clearProperty("zookeeper.admin.enableServer");
        System.clearProperty("zookeeper.4lw.commands.whitelist");
        System.clearProperty("java.security.auth.login.config");
        System.clearProperty("zookeeper.client.secure");
        System.clearProperty("zookeeper.ssl.trustStore.location");
        System.clearProperty("zookeeper.ssl.trustStore.password");
        System.clearProperty("zookeeper.ssl.trustStore.type");
    }

    @TempDir
    public Path baseDir;

    @Test
    public void testStart() throws Exception {

        int clientPort = PortManager.nextFreePort();
        int clientSecurePort = PortManager.nextFreePort();

        final Properties configZookeeper = new Properties();
        configZookeeper.put("clientPort", clientPort + "");
        configZookeeper.put("secureClientPort", clientSecurePort + "");
        configZookeeper.put("host", "localhost");
        configZookeeper.put("ticktime", "4000");
        // Netty is required for TLS
        configZookeeper.put("serverCnxnFactory", org.apache.zookeeper.server.NettyServerCnxnFactory.class.getName());

        File testKeyStore = new File("src/test/resources/org/apache/zookeeper/server/embedded/testKeyStore.jks");
        File testTrustStore = new File("src/test/resources/org/apache/zookeeper/server/embedded/testTrustStore.jks");
        assertTrue(testKeyStore.isFile());
        assertTrue(testTrustStore.isFile());
        configZookeeper.put("ssl.keyStore.location", testKeyStore.getAbsolutePath());
        configZookeeper.put("ssl.keyStore.password", "testpass");
        configZookeeper.put("ssl.keyStore.type", "JKS");

        System.setProperty("zookeeper.ssl.trustStore.location", testTrustStore.getAbsolutePath());
        System.setProperty("zookeeper.ssl.trustStore.password", "testpass");
        System.setProperty("zookeeper.ssl.trustStore.type", "JKS");

        try (ZooKeeperServerEmbedded zkServer = ZooKeeperServerEmbedded
                .builder()
                .baseDir(baseDir)
                .configuration(configZookeeper)
                .exitHandler(ExitHandler.DUMMY_EXIT())
                .build()) {
            zkServer.start();
            assertTrue(ClientBase.waitForServerUp("localhost:" + clientPort, 60000));
            for (int i = 0; i < 100; i++) {
                ZookeeperServeInfo.ServerInfo status = ZookeeperServeInfo.getStatus("StandaloneServer*");
                if (status.isIsleader() && status.isStandaloneMode()) {
                    break;
                }
                Thread.sleep(100);
            }
            ZookeeperServeInfo.ServerInfo status = ZookeeperServeInfo.getStatus("StandaloneServer*");
            assertTrue(status.isIsleader());
            assertTrue(status.isStandaloneMode());

            CountDownLatch l = new CountDownLatch(1);
            ZKClientConfig zKClientConfig = new ZKClientConfig();
            zKClientConfig.setProperty("zookeeper.client.secure", "true");
            // only netty supports TLS
            zKClientConfig.setProperty("zookeeper.clientCnxnSocket", org.apache.zookeeper.ClientCnxnSocketNetty.class.getName());
            try (ZooKeeper zk = new ZooKeeper("localhost:" + clientSecurePort, 60000, (WatchedEvent event) -> {
                switch (event.getState()) {
                    case SyncConnected:
                        l.countDown();
                        break;
                }
            }, zKClientConfig)) {
                assertTrue(zk.getClientConfig().getBoolean(ZKClientConfig.SECURE_CLIENT));
                assertTrue(l.await(10, TimeUnit.SECONDS));
            }

        }
    }

}
