package com.xjeffrose.chicago;

import com.xjeffrose.chicago.server.ChicagoServer;
import java.util.List;
import com.netflix.curator.test.TestingServer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.junit.Assert.*;

/**
 * Purpose: This fixture tests that leader election is working correctly.
 */
public class ChiLeaderSelectorListenerTest {

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  List<ChicagoServer> servers;
  CuratorFramework zk;
  TestingServer testingServer;

  @Before
  public void setup() throws Exception {
    testingServer = new TestingServer(2182);
    zk = CuratorFrameworkFactory.newClient(testingServer.getConnectString(), new ExponentialBackoffRetry(2000, 20));
    zk.start();
    servers = TestChicago.makeServers(TestChicago.chicago_dir(tmp), 4);
    for (ChicagoServer server : servers) {
      server.start();
    }
  }

  @After
  public void teardown() throws Exception {
    for (ChicagoServer server : servers) {
      server.stop();
    }
    zk.close();
    testingServer.close();
  }

  /**
   * Ensure that all of our nodes are participating in leader election
   */
  @Test
  public void takeLeadership() throws Exception {
    ZkClient zkClient = new ZkClient(zk);

    List<String> nodes = zkClient.getChildren("/chicago/chicago-elect");
    assertEquals(nodes.size(), servers.size());
  }
}
