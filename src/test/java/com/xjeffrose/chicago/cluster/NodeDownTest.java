package com.xjeffrose.chicago.cluster;

import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ListenableFuture;
import com.xjeffrose.chicago.TestChicago;
import com.xjeffrose.chicago.ZkClient;
import com.xjeffrose.chicago.client.ChicagoClient;
import com.xjeffrose.chicago.server.ChicagoServer;

import io.netty.util.internal.StringUtil;

import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by smadan on 6/8/16.
 */

public class NodeDownTest {
  TestingServer testingServer;
  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();
  HashMap<String,ChicagoServer> servers;
  ChicagoClient chicagoClient;

  @Before
  public void setup() throws Exception {
    InstanceSpec spec = new InstanceSpec(null, 2182,  -1 , -1, true, -1 , 20 , -1);
    testingServer = new TestingServer(spec,true);
    servers = TestChicago.makeNamedServers(TestChicago.chicago_dir(tmp), 4, testingServer.getConnectString());
    for (String server : servers.keySet()) {
      servers.get(server).start();
    }

    chicagoClient = new ChicagoClient(testingServer.getConnectString(), 3);
    chicagoClient.startAndWaitForNodes(3);
  }

  @After
  public void teardown() throws Exception {
    for (String server : servers.keySet()) {
      servers.get(server).stop();
    }
    chicagoClient.stop();
    testingServer.stop();
    servers.clear();
    chicagoClient =null;
  }

  @Test
  public void nodeDownTest() throws Exception {
    byte[] offset = null;
    String key = "tskey";

    System.out.println("Clients : " + chicagoClient.getNodeList(key.getBytes()).toString());

    for (int i = 0; i < 30; i++) {
      String _v = "val" + i;
      byte[] val = _v.getBytes();
      if (i == 9) {
        offset = chicagoClient.tsWrite(key.getBytes(), val).get().get(0);
      }else {
        assertNotNull(chicagoClient.tsWrite(key.getBytes(), val).get().get(0));
      }

    }

    System.out.println("On normal state : ");
    printStrem(key, Longs.toByteArray(0));

    //Restart chicago1 intermittently
    ExecutorService executor = Executors.newFixedThreadPool(10);
     Future restartTask = executor.submit(() -> {
       for(int i=0;i<2;i++) {
         int random = (int) (Math.random() * 4 + 1);
         String server = "chicago1";
         try {
           stopServer(server);
           Thread.sleep(500);
           printStrem(key, null);
           startServer(server);
           printStrem(key, null);
           Thread.sleep(500);
         } catch (Exception e) {
           e.printStackTrace();
         }
       }
    });

    restartTask.get();
    executor.shutdown();
  }

  public void stopServer(String server) throws Exception{
    //BringDown one server
    ZkClient zk = new ZkClient(testingServer.getConnectString(),false);
    zk.start();
    System.out.println("Stopping server : "+servers.get(server).config.getDBBindEndpoint());
    zk.delete("/chicago/node-list/" + servers.get(server).config.getDBBindEndpoint());
    zk.stop();
  }

  public void startServer(String server) throws Exception {
    //BringDown one server
    ZkClient zk = new ZkClient(testingServer.getConnectString(),false);
    zk.start();
    System.out.println("Starting server : "+ servers.get(server).config.getDBBindEndpoint());
    zk.set("/chicago/node-list/"+ servers.get(server).config.getDBBindEndpoint(), "");
    zk.stop();
  }

  public void printStrem(String key, byte[] offset) throws Exception {
    System.out.println("Clients : " + chicagoClient.getNodeList(key.getBytes()).toString());

    String result = new String(chicagoClient.stream(key.getBytes(), offset).get().get(0));
    assertEquals(true, !StringUtil.isNullOrEmpty(result));
    System.out.println(result);
  }

}
