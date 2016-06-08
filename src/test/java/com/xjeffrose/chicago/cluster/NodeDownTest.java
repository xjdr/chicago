package com.xjeffrose.chicago.cluster;

import com.google.common.collect.HashMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.curator.test.TestingServer;
import com.xjeffrose.chicago.TestChicago;
import com.xjeffrose.chicago.client.ChicagoClient;
import com.xjeffrose.chicago.client.ChicagoStream;
import com.xjeffrose.chicago.client.ChicagoTSClient;
import com.xjeffrose.chicago.server.ChicagoServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertNotNull;

/**
 * Created by smadan on 6/8/16.
 */
public class NodeDownTest {
  TestingServer testingServer;
  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();
  HashMap<String,ChicagoServer> servers;
  ChicagoTSClient chicagoTSClient;

  @Before
  public void setup() throws Exception {
    testingServer = new TestingServer(2182);
    servers = TestChicago.makeServers(TestChicago.chicago_dir(tmp), 4);
    for (String server : servers.keySet()) {
      servers.get(server).start();
    }

    chicagoTSClient = new ChicagoTSClient(testingServer.getConnectString(), 3);
    chicagoTSClient.startAndWaitForNodes(4);
  }

  @After
  public void teardown() throws Exception {
    for (String server : servers.keySet()) {
      servers.get(server).stop();
    }
    chicagoTSClient.stop();
    testingServer.stop();
  }

  @Test
  public void transactStream() throws Exception {
    byte[] offset = null;
    String key = "tskey";

    System.out.println(chicagoTSClient.getNodeList(key.getBytes()).toString());

    for (int i = 0; i < 20; i++) {
      String _v = "val" + i;
      byte[] val = _v.getBytes();
      if (i == 12) {
        offset = chicagoTSClient.write(key.getBytes(), val);
      }
      assertNotNull(chicagoTSClient.write(key.getBytes(), val));
      System.out.println(i);
    }

  }

}
