package com.xjeffrose.chicago;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ZkClientTest {
  TestingServer testingServer;
  CuratorFramework curatorFramework;
  ZkClient zkClient;

  @Before
  public void setUp() throws Exception {

    this.testingServer = new TestingServer();
    this.curatorFramework = CuratorFrameworkFactory.newClient(testingServer.getConnectString(), new ExponentialBackoffRetry(500, 4));
    this.zkClient = new ZkClient(testingServer.getConnectString(), true);

    this.testingServer.start();
    this.curatorFramework.start();
    this.zkClient.start();
  }

  @After
  public void tearDown() throws Exception {
    this.testingServer.stop();
    this.curatorFramework.close();
    this.zkClient.stop();
  }

  @Test
  public void register() throws Exception {
//    zkClient.register();
  }

  @Test
  public void electLeader() throws Exception {
  }

  @Test
  public void isLeader() throws Exception {

  }

  @Test
  public void set() throws Exception {
    zkClient.set("/test/set", "foo");

    assertEquals("foo", new String(curatorFramework.getData().forPath("/test/set")));
  }

  @Test
  public void get() throws Exception {
    curatorFramework.create().creatingParentsIfNeeded().forPath("/test/get", "foo".getBytes());

    assertEquals("foo", zkClient.get("/test/get"));
  }

  @Test
  public void list() throws Exception {

  }

  @Test
  public void getChildren() throws Exception {

  }

  @Test
  public void createIfNotExist() throws Exception {

  }

  @Test
  public void delete() throws Exception {

  }

  @Test
  public void createLockPath() throws Exception {

  }

  @Test
  public void deleteLockPath() throws Exception {

  }

  @Test
  public void getClient() throws Exception {

  }

}
