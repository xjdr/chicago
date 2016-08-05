package com.xjeffrose.chicago.client;

import com.xjeffrose.chicago.TestChicago;
import com.xjeffrose.chicago.server.ChicagoServer;
import com.google.common.collect.ImmutableList;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;

public class ClientNodeWatcherTest {

  TestingServer testingServer;
  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();
  List<ChicagoServer> servers;
  ChicagoClient chicagoClientDHT;

  @Before
  public void setup() throws Exception {
    InstanceSpec spec = new InstanceSpec(null, -1,  -1 , -1, true, -1 , 20 , -1);
    testingServer = new TestingServer(spec,true);
    servers = TestChicago.makeServers(TestChicago.chicago_dir(tmp), 4, testingServer.getConnectString());

    for (ChicagoServer server : servers) {
      server.start();
    }

    chicagoClientDHT = new ChicagoClient(testingServer.getConnectString(), 3);
    chicagoClientDHT.startAndWaitForNodes(4);
  }

  @After
  public void teardown() throws Exception {
    for (ChicagoServer server : servers) {
      server.stop();
    }
    chicagoClientDHT.stop();
    testingServer.stop();
  }


  @Test
  public void removeSingleNode() throws Exception {

    while (chicagoClientDHT.getNodeList("foo".getBytes()) == null) {
      Thread.sleep(1);
    }

    List<String> before = ImmutableList.copyOf(chicagoClientDHT.getNodeList("foo".getBytes()));

    servers.get(0).stop();

    Thread.sleep(3000);

    List<String> after = chicagoClientDHT.getNodeList("foo".getBytes());

    assertTrue(chicagoClientDHT.buildNodeList().containsAll(after));

//    assertTrue(Collections.disjoint(before, after));

    servers.get(4).start();

    Thread.sleep(3000);

    assertEquals(3, chicagoClientDHT.getNodeList("foo".getBytes()).size());
  }
}
