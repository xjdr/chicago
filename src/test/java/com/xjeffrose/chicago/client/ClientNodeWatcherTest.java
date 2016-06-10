package com.xjeffrose.chicago.client;

import com.xjeffrose.chicago.TestChicago;
import com.xjeffrose.chicago.server.ChicagoServer;
import com.google.common.collect.ImmutableList;
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
    testingServer = new TestingServer(true);
    servers = TestChicago.makeServers(TestChicago.chicago_dir(tmp), 5, testingServer.getConnectString());
    servers.get(0).start();
    servers.get(1).start();
    servers.get(2).start();
    servers.get(3).start();

    chicagoClientDHT = new ChicagoClient(testingServer.getConnectString(), 3);
    chicagoClientDHT.startAndWaitForNodes(4);
  }

  @After
  public void teardown() throws Exception {
    servers.get(1).stop();
    servers.get(2).stop();
    servers.get(3).stop();
    servers.get(4).stop();
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
