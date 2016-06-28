package com.xjeffrose.chicago.client;

import com.google.common.primitives.Ints;
import com.xjeffrose.chicago.TestChicago;
import com.xjeffrose.chicago.ZkClient;
import com.xjeffrose.chicago.server.ChicagoServer;

import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by root on 6/21/16.
 */

public class ReplicationLockTest {
  TestingServer testingServer;
  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();
  List<ChicagoServer> servers;
  ChicagoTSClient chicagoTSClient;

  @Before
  public void setup() throws Exception {
    InstanceSpec spec = new InstanceSpec(null, 2182,  -1 , -1, true, -1 , 3000 , -1);
    testingServer = new TestingServer(spec,true);
    servers = TestChicago.makeServers(TestChicago.chicago_dir(tmp), 4, testingServer.getConnectString());
    for (ChicagoServer server : servers) {
      server.start();
    }

    chicagoTSClient = new ChicagoTSClient(testingServer.getConnectString(), 3);
    chicagoTSClient.startAndWaitForNodes(3);
  }

  @After
  public void teardown() throws Exception {
    for (ChicagoServer server : servers) {
      server.stop();
    }
    chicagoTSClient.stop();
    testingServer.stop();
  }

  @Test
  public void simulateReplication() throws Exception {
    String key = "testKey";
    List<String> servers =  chicagoTSClient.getNodeList(key.getBytes());
    System.out.println("Servers for key : " + servers.toString());
    String val = "testVal";
    int insertedKey  = Ints.fromByteArray(chicagoTSClient.write(key.getBytes(),val.getBytes()));
    assertTSClient(key,insertedKey,val,servers);


    //Insert one node in replication lock
    ZkClient zk = new ZkClient(testingServer.getConnectString());
    zk.start();
    val = "newVal";
    boolean created  = zk.createIfNotExist(ChicagoServer.NODE_LOCK_PATH+"/"+key+"/"+servers.get(0), "");
    if(!created){
      System.exit(-1);
    }
    System.out.println("Created the replication path.");
    assertEquals(2,chicagoTSClient.getEffectiveNodes(key.getBytes()).size());
    insertedKey  = Ints.fromByteArray(chicagoTSClient.write(key.getBytes(),val.getBytes()));
    String removedServer = servers.remove(0);

    //Value should be present in only 2 nodes
    assertTSClient(key,insertedKey,val,servers);


    //Value should not be present in the node being replicated
    ChicagoClient cc = new ChicagoClient(removedServer);
    try {
      System.out.println("Trying to get the value from bad node");
      String futureval = new String(cc.read(key.getBytes(), Ints.toByteArray(insertedKey)).get());
      assertTrue(futureval.equals(""));
    }catch(Exception e){
      e.printStackTrace();
      System.out.println("countdown catch");
    }

  }

  public void assertTSClient(String colFam, int key, String val, List<String> nodes){
    nodes.forEach(n -> {
      System.out.println("Checking node "+n);
      try {
        ChicagoClient cc = new ChicagoClient(n);
        assertTrue(val.equals(new String(cc.read(colFam.getBytes(), Ints.toByteArray(key)).get())));
        cc.stop();
      }catch (Exception e){
        e.printStackTrace();
        return;
      }
    });
  }
}
