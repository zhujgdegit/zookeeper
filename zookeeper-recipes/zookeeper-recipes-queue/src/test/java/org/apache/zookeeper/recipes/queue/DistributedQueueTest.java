/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zookeeper.recipes.queue;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import java.util.NoSuchElementException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.test.ClientBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class DistributedQueueTest extends ClientBase {

    @AfterEach
    public void tearDown() throws Exception {
        super.tearDown();
    }


    @Test
    public void testOffer1() throws Exception {
        String dir = "/testOffer1";
        String testString = "Hello World";
        final int num_clients = 1;
        ZooKeeper[] clients = new ZooKeeper[num_clients];
        DistributedQueue[] queueHandles = new DistributedQueue[num_clients];
        for(int i=0; i < clients.length; i++){
            clients[i] = createClient();
            queueHandles[i] = new DistributedQueue(clients[i], dir, null);
        }

        queueHandles[0].offer(testString.getBytes(UTF_8));

        byte[] dequeuedBytes = queueHandles[0].remove();
<<<<<<< HEAD
        assertEquals(new String(dequeuedBytes), testString);
=======
        Assert.assertEquals(new String(dequeuedBytes, UTF_8), testString);
>>>>>>> ZOOKEEPER-3342: Use StandardCharsets
    }

    @Test
    public void testOffer2() throws Exception {
        String dir = "/testOffer2";
        String testString = "Hello World";
        final int num_clients = 2;
        ZooKeeper[] clients = new ZooKeeper[num_clients];
        DistributedQueue[] queueHandles = new DistributedQueue[num_clients];
        for(int i=0; i < clients.length; i++){
            clients[i] = createClient();
            queueHandles[i] = new DistributedQueue(clients[i], dir, null);
        }

        queueHandles[0].offer(testString.getBytes(UTF_8));

        byte[] dequeuedBytes = queueHandles[1].remove();
<<<<<<< HEAD
        assertEquals(new String(dequeuedBytes), testString);
=======
        Assert.assertEquals(new String(dequeuedBytes, UTF_8), testString);
>>>>>>> ZOOKEEPER-3342: Use StandardCharsets
    }

    @Test
    public void testTake1() throws Exception {
        String dir = "/testTake1";
        String testString = "Hello World";
        final int num_clients = 1;
        ZooKeeper[] clients = new ZooKeeper[num_clients];
        DistributedQueue[] queueHandles = new DistributedQueue[num_clients];
        for(int i=0; i < clients.length; i++){
            clients[i] = createClient();
            queueHandles[i] = new DistributedQueue(clients[i], dir, null);
        }

        queueHandles[0].offer(testString.getBytes(UTF_8));

        byte[] dequeuedBytes = queueHandles[0].take();
<<<<<<< HEAD
        assertEquals(new String(dequeuedBytes), testString);
=======
        Assert.assertEquals(new String(dequeuedBytes, UTF_8), testString);
>>>>>>> ZOOKEEPER-3342: Use StandardCharsets
    }



    @Test
    public void testRemove1() throws Exception{
        String dir = "/testRemove1";
        String testString = "Hello World";
        final int num_clients = 1;
        ZooKeeper[] clients = new ZooKeeper[num_clients];
        DistributedQueue[] queueHandles = new DistributedQueue[num_clients];
        for(int i=0; i < clients.length; i++){
            clients[i] = createClient();
            queueHandles[i] = new DistributedQueue(clients[i], dir, null);
        }

        try{
            queueHandles[0].remove();
        }catch(NoSuchElementException e){
            return;
        }
<<<<<<< HEAD

        fail();
=======
        Assert.assertTrue(false);
>>>>>>> ZOOKEEPER-3342: Use StandardCharsets
    }

    public void createNremoveMtest(String dir,int n,int m) throws Exception{
        String testString = "Hello World";
        final int num_clients = 2;
        ZooKeeper[] clients = new ZooKeeper[num_clients];
        DistributedQueue[] queueHandles = new DistributedQueue[num_clients];
        for(int i=0; i < clients.length; i++){
            clients[i] = createClient();
            queueHandles[i] = new DistributedQueue(clients[i], dir, null);
        }

        for(int i=0; i< n; i++){
            String offerString = testString + i;
            queueHandles[0].offer(offerString.getBytes(UTF_8));
        }

        byte[] data = null;
        for(int i=0; i<m; i++){
            data=queueHandles[1].remove();
        }
<<<<<<< HEAD

        assertNotNull(data);
        assertEquals(new String(data), testString + (m - 1));
=======
        Assert.assertEquals(new String(data, UTF_8), testString+(m-1));
>>>>>>> ZOOKEEPER-3342: Use StandardCharsets
    }

    @Test
    public void testRemove2() throws Exception{
        createNremoveMtest("/testRemove2",10,2);
    }
    @Test
    public void testRemove3() throws Exception{
        createNremoveMtest("/testRemove3",1000,1000);
    }

    public void createNremoveMelementTest(String dir, int n, int m) throws Exception {
        String testString = "Hello World";
        final int num_clients = 2;
        ZooKeeper[] clients = new ZooKeeper[num_clients];
        DistributedQueue[] queueHandles = new DistributedQueue[num_clients];
        for(int i=0; i < clients.length; i++){
            clients[i] = createClient();
            queueHandles[i] = new DistributedQueue(clients[i], dir, null);
        }

        for(int i=0; i< n; i++){
            String offerString = testString + i;
            queueHandles[0].offer(offerString.getBytes(UTF_8));
        }

        byte[] data = null;
        for(int i=0; i<m; i++){
            data=queueHandles[1].remove();
        }
<<<<<<< HEAD
        assertEquals(new String(queueHandles[1].element()), testString + m);
=======
        Assert.assertEquals(new String(queueHandles[1].element(), UTF_8), testString+m);
>>>>>>> ZOOKEEPER-3342: Use StandardCharsets
    }

    @Test
    public void testElement1() throws Exception {
        createNremoveMelementTest("/testElement1",1,0);
    }

    @Test
    public void testElement2() throws Exception {
        createNremoveMelementTest("/testElement2",10,2);
    }

    @Test
    public void testElement3() throws Exception {
        createNremoveMelementTest("/testElement3",1000,500);
    }

    @Test
    public void testElement4() throws Exception {
        createNremoveMelementTest("/testElement4",1000,1000-1);
    }

    @Test
    public void testTakeWait1() throws Exception{
        String dir = "/testTakeWait1";
        final String testString = "Hello World";
        final int num_clients = 1;
        final ZooKeeper[] clients = new ZooKeeper[num_clients];
        final DistributedQueue[] queueHandles = new DistributedQueue[num_clients];
        for(int i=0; i < clients.length; i++){
            clients[i] = createClient();
            queueHandles[i] = new DistributedQueue(clients[i], dir, null);
        }

        final byte[] takeResult[] = new byte[1][];
        Thread takeThread = new Thread(){
            public void run(){
                try{
                    takeResult[0] = queueHandles[0].take();
                }catch(KeeperException e){

                }catch(InterruptedException e){

                }
            }
        };
        takeThread.start();

        Thread.sleep(1000);
        Thread offerThread= new Thread() {
            public void run(){
                try {
                    queueHandles[0].offer(testString.getBytes(UTF_8));
                } catch (KeeperException e) {

                } catch (InterruptedException e) {

                }
            }
        };
        offerThread.start();
        offerThread.join();

        takeThread.join();

<<<<<<< HEAD
        assertNotNull(takeResult[0]);
        assertEquals(new String(takeResult[0]), testString);
=======
        Assert.assertTrue(takeResult[0] != null);
        Assert.assertEquals(new String(takeResult[0], UTF_8), testString);
>>>>>>> ZOOKEEPER-3342: Use StandardCharsets
    }

    @Test
    public void testTakeWait2() throws Exception{
        String dir = "/testTakeWait2";
        final String testString = "Hello World";
        final int num_clients = 1;
        final ZooKeeper[] clients = new ZooKeeper[num_clients];
        final DistributedQueue[] queueHandles = new DistributedQueue[num_clients];
        for(int i=0; i < clients.length; i++){
            clients[i] = createClient();
            queueHandles[i] = new DistributedQueue(clients[i], dir, null);
        }
        int num_attempts =2;
        for(int i=0; i< num_attempts; i++){
            final byte[] takeResult[] = new byte[1][];
            final String threadTestString = testString + i;
            Thread takeThread = new Thread(){
                public void run(){
                    try{
                        takeResult[0] = queueHandles[0].take();
                    }catch(KeeperException e){

                    }catch(InterruptedException e){

                    }
                }
            };
            takeThread.start();

            Thread.sleep(1000);
            Thread offerThread= new Thread() {
                public void run(){
                    try {
                        queueHandles[0].offer(threadTestString.getBytes(UTF_8));
                    } catch (KeeperException e) {

                    } catch (InterruptedException e) {

                    }
                }
            };
            offerThread.start();
            offerThread.join();

            takeThread.join();

<<<<<<< HEAD
            assertNotNull(takeResult[0]);
            assertEquals(new String(takeResult[0]), threadTestString);
=======
            Assert.assertTrue(takeResult[0] != null);
            Assert.assertEquals(new String(takeResult[0], UTF_8), threadTestString);
>>>>>>> ZOOKEEPER-3342: Use StandardCharsets
        }
    }

}

