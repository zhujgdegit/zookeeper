package org.apache.zookeeper.test;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DataTree;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class CreateContainerTest extends ClientBase {
    private ZooKeeper zk;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        zk = createClient();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        zk.close();
    }

    @Test
    public void testCreate()
            throws IOException, KeeperException, InterruptedException {
        createNoStatVerifyResult("/foo");
        createNoStatVerifyResult("/foo/child");
    }

    @Test
    public void testCreateWithStat()
            throws IOException, KeeperException, InterruptedException {
        Stat stat = createWithStatVerifyResult("/foo");
        Stat childStat = createWithStatVerifyResult("/foo/child");
        // Don't expect to get the same stats for different creates.
        Assert.assertFalse(stat.equals(childStat));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testCreateWithNullStat()
            throws IOException, KeeperException, InterruptedException {
        final String name = "/foo";
        Assert.assertNull(zk.exists(name, false));

        Stat stat = null;
        // If a null Stat object is passed the create should still
        // succeed, but no Stat info will be returned.
        zk.createContainer(name, name.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                stat);
        Assert.assertNull(stat);
        Assert.assertNotNull(zk.exists(name, false));

        zk.delete("/foo", -1);
    }

    private void createNoStatVerifyResult(String newName)
            throws KeeperException, InterruptedException {
        Assert.assertNull("Node existed before created", zk.exists(newName, false));
        String path = zk.createContainer(newName, newName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Assert.assertEquals(path, newName);
        Assert.assertNotNull("Node was not created as expected",
                zk.exists(newName, false));
    }
    private Stat createWithStatVerifyResult(String newName)
            throws KeeperException, InterruptedException {
        Assert.assertNull("Node existed before created", zk.exists(newName, false));
        Stat stat = new Stat();
        String path = zk.createContainer(newName, newName.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, stat);
        Assert.assertEquals(path, newName);
        validateCreateStat(stat, newName);

        Stat referenceStat = zk.exists(newName, false);
        Assert.assertNotNull("Node was not created as expected", referenceStat);
        Assert.assertEquals(referenceStat, stat);

        return stat;
    }

    private void validateCreateStat(Stat stat, String name) {
        Assert.assertEquals(stat.getCzxid(), stat.getMzxid());
        Assert.assertEquals(stat.getCzxid(), stat.getPzxid());
        Assert.assertEquals(stat.getCtime(), stat.getMtime());
        Assert.assertEquals(0, stat.getCversion());
        Assert.assertEquals(0, stat.getVersion());
        Assert.assertEquals(0, stat.getAversion());
        Assert.assertEquals(DataTree.CONTAINER_EPHEMERAL_OWNER, stat.getEphemeralOwner());
        Assert.assertEquals(name.length(), stat.getDataLength());
        Assert.assertEquals(0, stat.getNumChildren());
    }
}
