package com.smadan.chicago;

import com.google.common.primitives.Longs;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.xjeffrose.chicago.client.ChicagoAsyncClient;
import org.junit.Before;
import org.junit.runners.Parameterized;
import org.junit.Test;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by root on 8/10/16.
 */
public class ReplicationTest {
  TestChicagoCluster testChicagoCluster;
  HashMap<String, String> servers = new HashMap<>();

  @Before
  public void setup() throws Exception {
    Config config = ConfigFactory.load("test").getConfig("testing-servers");

    for(String server: config.getString("servers").split(",")){
      String serverEndpoint=server.concat(":").concat(config.getString("dbport"));
      servers.put(serverEndpoint,serverEndpoint);
    }

    String zkString = config.getString("zkstring");
    testChicagoCluster = new TestChicagoCluster(servers, zkString, 3);
  }
  public String forServer(String server) {
    String result = null;
    for (String k : servers.keySet()) {
      String s = servers.get(k);
      if (s.equals(server)) {
        result = k;
      }
    }
    return result;
  }

  public void testValidResponse(List<String> nodes, String colFam, int key) throws Exception{
    ChicagoAsyncClient cc=testChicagoCluster.chicagoClientHashMap.get(nodes.get(0));
    String response1 = new String(cc.read(colFam.getBytes(), Longs.toByteArray(key)).get());
    cc=testChicagoCluster.chicagoClientHashMap.get(nodes.get(1));
    String response2 = new String(cc.read(colFam.getBytes(), Longs.toByteArray(key)).get());
    cc=testChicagoCluster.chicagoClientHashMap.get(nodes.get(2));
    String response3 = new String(cc.read(colFam.getBytes(), Longs.toByteArray(key)).get());
    String expectedResponse="val"+key;
    assertEquals(response1,(expectedResponse));
    assertEquals(response2,( expectedResponse));
    assertEquals(response3,( expectedResponse));
    System.out.println("Response is valid");
  }

  public void write000Values(String colFam) throws Exception {
    for (int i = 0; i < 1500; i++) {
      String _v = "val" + i;
      byte[] val = _v.getBytes();
      byte[] ret = testChicagoCluster.chicagoClient.tsWrite(colFam.getBytes(), val).get();
      assertTrue(Longs.fromByteArray(ret) >= 0 && Longs.fromByteArray(ret) < 1500);
    }
  }

  public void testReplicationOnNodesAfterWaitAA() throws Exception {
    String colFam = "testReplicationKey";

    deleteColFam(colFam);

    write000Values(colFam);

    List<String> nodes = testChicagoCluster.chicagoClient.getNodeList(colFam.getBytes());
    System.out.println("Querying old set of nodes" + nodes.toString());
    testValidResponse(nodes,colFam,100);
    testValidResponse(nodes,colFam, 999);

    for (String node : nodes) {
      System.out.println("Stopping server.. "
        + node);
      testChicagoCluster.zkClient.delete(testChicagoCluster.NODE_LIST_PATH +"/"+ node);
      break;
    }

    List<String> replicationList = null;
    System.out.println("Waiting for replication lock to be created");
    replicationList = testChicagoCluster.zkClient.list(testChicagoCluster.NODE_LOCK_PATH + "/"
      + colFam);

    while  (replicationList.isEmpty()){
      Thread.sleep(1);
      //do nothing, wait for the lock path to get created
      replicationList = testChicagoCluster.zkClient.list(testChicagoCluster.NODE_LOCK_PATH + "/"
        + colFam);
    }

    System.out.println("This is the list being replicated right now "+ replicationList.toString());

    System.out.println("Waiting for replication to terminate");
    replicationList = testChicagoCluster.zkClient.list(testChicagoCluster.NODE_LOCK_PATH + "/"
      + colFam);
    while (!replicationList.isEmpty()){
      Thread.sleep(1);
      //do nothing, wait for the lock path to get deleted
      replicationList = testChicagoCluster.zkClient.list(testChicagoCluster.NODE_LOCK_PATH + "/"
        + new String(colFam));
    }
    System.out.println("Replication completed!!");
    Thread.sleep(2000);
    List<String> newNodes = testChicagoCluster.chicagoClient.getNodeList(colFam.getBytes());
    System.out.println("New nodes = " + newNodes.toString());
    //Test after replication is completed
    for (int i = 1; i < 5; i++) {
      System.out.println(i + "th iteration - Querying new set of nodes"
        + nodes.toString());
      testValidResponse(newNodes,colFam,100);
      testValidResponse(newNodes,colFam,999);
    }

    testChicagoCluster.chicagoClient.deleteColFam(colFam.getBytes());
  }

  public void assertTSClient(String colFam, int key, String val){
    List<String> nodes = testChicagoCluster.chicagoClient.getEffectiveNodes(colFam.getBytes());
    nodes.forEach(n -> {
      System.out.println("Checking node "+n);
      try {
        ChicagoAsyncClient cc = new ChicagoAsyncClient(n);
        cc.start();
          Thread.sleep(1000);
        assertEquals(val,new String(cc.read(colFam.getBytes(), Longs.toByteArray(key)).get()));
      }catch (Exception e){
        e.printStackTrace();
        return;
      }
    });
  }

  @Test @Parameterized.Parameters
  public void testReplication() throws Exception{
    String tsKey = "tsRepKeyNew";
    try {
      deleteColFam(tsKey);
      List<String> nodes = testChicagoCluster.chicagoClient.getNodeList(tsKey.getBytes());
      assert (true == !testChicagoCluster.zkClient.list("/chicago/node-list").isEmpty());
      //Write some data.
      for (int i = 0; i < 2000; i++) {
        String _v = "val" + i;
        byte[] val = _v.getBytes();
        testChicagoCluster.chicagoClient.tsWrite(tsKey.getBytes(), val).get();
        if (i % 50 == 0) System.out.println(i);
      }
      assertTSClient(tsKey, 90, "val90");
      assertTSClient(tsKey, 1999, "val1999");

      //Bring down the node.
      String server = nodes.get(0).split(":")[0];
      System.out.println("Shutting down " + server);
      String command = "sudo kill $(ps aux|  grep 'chicago'  | awk '{print $2}')";
      remoteExec(server, command);

      //Check for node in Zookeeper.
      long startTime = System.currentTimeMillis();
      String path = testChicagoCluster.NODE_LOCK_PATH + "/" + tsKey;
      //Wait for replication to happen
      List<String> repNodes = testChicagoCluster.zkClient.list(path);
      while (repNodes.size() < 2) {
        repNodes = testChicagoCluster.zkClient.list(path);
      }
      System.out.println("Lock path populated in "
        + (System.currentTimeMillis() - startTime)
        + "ms  repNode :"
        + repNodes.toString());

      assertTSClient(tsKey, 90, "val90");
      repNodes = testChicagoCluster.zkClient.list(path);
      while (!repNodes.isEmpty()) {
        Thread.sleep(10);
        repNodes = testChicagoCluster.zkClient.list(path);
      }
      //Thread.sleep(2000);
      assertTSClient(tsKey, 90, "val90");

      //Bring the server back up again.
      System.out.println("Starting server " + server);
      command = "sudo sh -c \"cd /home/smadan/chicago; ./bin/chiServer.sh &\"";
      remoteExec(server, command);

      Thread.sleep(2000);
      assertTSClient(tsKey, 1999, "val1999");
    }catch ( Exception e){
      e.printStackTrace();
      throw e;
    }finally {
      //deleteColFam(tsKey);
    }
  }

  public void deleteColFam(String colFam){
    for(ChicagoAsyncClient cc : testChicagoCluster.chicagoClientHashMap.values()){
      try {
        cc.deleteColFam(colFam.getBytes());
      }catch(Exception e){
        e.printStackTrace();
      }
    }
  }


  public void remoteExec(String ip, String command) throws Exception{
    String host=ip;
    String user="prodngdev";
    String command1=command;
    String password = "kenobi";
    try{

      java.util.Properties config = new java.util.Properties();
      config.put("StrictHostKeyChecking", "no");
      JSch jsch = new JSch();
      Session session=jsch.getSession(user, host, 22);
      session.setPassword(password);
      session.setConfig(config);
      session.connect();
      System.out.println("Connected");

      Channel channel=session.openChannel("exec");
      ((ChannelExec)channel).setCommand(command1);
      channel.setInputStream(null);
      ((ChannelExec)channel).setErrStream(System.err);

      InputStream in=channel.getInputStream();
      channel.connect();
      byte[] tmp=new byte[1024];
      long startTime = System.currentTimeMillis();
      while(true){
        if(System.currentTimeMillis() - startTime > 2000){
          break;
        }
        while(in.available()>0){
          int i=in.read(tmp, 0, 1024);
          if(i<0)break;
          System.out.print(new String(tmp, 0, i));
        }
        if(channel.isClosed()){
          System.out.println("exit-status: "+channel.getExitStatus());
          break;
        }
        try{Thread.sleep(1000);}catch(Exception ee){}
      }
      channel.disconnect();
      session.disconnect();
      System.out.println("DONE");
    }catch(Exception e){
      e.printStackTrace();
    }
  }


}
