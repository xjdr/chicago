package com.xjeffrose.chicago;

import com.netflix.curator.test.TestingServer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;

public class ChiLeaderSelectorListenerTest {

  @Test
  public void takeLeadership() throws Exception {
    TestingServer testingServer = new TestingServer(2182);

    new Chicago().main(new String[]{"", "src/test/resources/test1.conf"});
    new Chicago().main(new String[]{"", "src/test/resources/test2.conf"});
    new Chicago().main(new String[]{"", "src/test/resources/test3.conf"});
    new Chicago().main(new String[]{"", "src/test/resources/test4.conf"});

    CuratorFramework zk = CuratorFrameworkFactory.newClient(testingServer.getConnectString(), new ExponentialBackoffRetry(2000, 20));
    zk.start();

    ZkClient zkClient = new ZkClient(zk);

    zkClient.getChildren("/chicago/chicago-elect").forEach(xs -> {
      System.out.println(xs);
      System.out.println(zkClient.get("/chicago/chicago-elect/" + xs));
    });
  }

}