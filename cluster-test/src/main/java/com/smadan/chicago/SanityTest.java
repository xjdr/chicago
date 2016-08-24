package com.smadan.chicago;

import com.google.common.primitives.Longs;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.xjeffrose.chicago.ChiUtil;
import com.xjeffrose.chicago.client.ChicagoAsyncClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by root on 8/10/16.
 */
public class SanityTest {
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

  @Test
  public void transactOnce() throws Exception {
    for (int i = 0; i < 1; i++) {
      String _k = "key" + i;
      byte[] key = _k.getBytes();
      String _v = "val" + i;
      byte[] val = _v.getBytes();

      Assert.assertNotNull((testChicagoCluster.chicagoClient.write(key, val).get()));
      byte[] result = testChicagoCluster.chicagoClient.read(key).get();
      assertEquals(new String(val),new String(result));
    }
  }

  @Test
  public void transactOnceTS() throws Exception {
    String colFam = "testTS";
    testChicagoCluster.chicagoClient.deleteColFam(colFam.getBytes());
    for (int i = 0; i < 1; i++) {
      String _k = "key" + i;
      byte[] key = _k.getBytes();
      String _v = "val" + i;
      byte[] val = _v.getBytes();

      byte[] offset  = testChicagoCluster.chicagoClient.tsWrite(colFam.getBytes(), val).get();
      byte[] result = testChicagoCluster.chicagoClient.read(colFam.getBytes(),offset).get();
      assertEquals(new String(val),new String(result));
    }
    testChicagoCluster.chicagoClient.deleteColFam(colFam.getBytes());
  }

  @Test
  public void TSWriteWithKey() throws Exception {
    String colFam = "testTS";
    testChicagoCluster.chicagoClient.deleteColFam(colFam.getBytes());
    for (int i = 0; i < 1; i++) {
      byte[] key = Longs.toByteArray(2l);
      String _v = "val" + i;
      byte[] val = _v.getBytes();

      byte[] offset  = testChicagoCluster.chicagoClient.tsWrite(colFam.getBytes(),key, val).get();

      byte[] result = testChicagoCluster.chicagoClient.read(colFam.getBytes(),key).get();
      assertEquals(new String(val),new String(result));
    }
    testChicagoCluster.chicagoClient.deleteColFam(colFam.getBytes());
  }

  @Test
  public void TSWriteWithKeySingleClient() throws Exception {
    String colFam = "testTS";
    for(String ccs : testChicagoCluster.chicagoClientHashMap.keySet()){
      System.out.println("Writing to "+ ccs);
      ChicagoAsyncClient cc = testChicagoCluster.chicagoClientHashMap.get(ccs);
      cc.deleteColFam(colFam.getBytes());
      byte[] key = Longs.toByteArray(2l);
      String _v = "valSometing";
      byte[] val = _v.getBytes();

      byte[] offset  = cc.tsWrite(colFam.getBytes(),key, val).get();

      byte[] result = cc.read(colFam.getBytes(),key).get();
      assertEquals(new String(val),new String(result));
      cc.deleteColFam(colFam.getBytes());
    }
  }

  @Test
  public void TSWriteWithNewClient() throws Exception {
    String colFam = "testTS";
    for(String ccs : testChicagoCluster.chicagoClientHashMap.keySet()){
      System.out.println("Writing to "+ ccs);
      ChicagoAsyncClient cc = new ChicagoAsyncClient(ccs);
      cc.start();
      cc.deleteColFam(colFam.getBytes());
      byte[] key = Longs.toByteArray(2l);
      String _v = "valSometing";
      byte[] val = _v.getBytes();

      byte[] offset  = cc.tsWrite(colFam.getBytes(),key, val).get();

      byte[] result = cc.read(colFam.getBytes(),key).get();
      assertEquals(new String(val),new String(result));
      cc.deleteColFam(colFam.getBytes());
    }
  }


  @Test
  public void writeTSSequence() throws Exception{
    byte[] offset = null;
    String tsKey = "testKey";
    testChicagoCluster.chicagoClient.deleteColFam(tsKey.getBytes());
    List<String> nodes = testChicagoCluster.chicagoClient.getNodeList(tsKey.getBytes());
    for (int i = 0; i < 30; i++) {
      String _v = "val" + i;
      byte[] val = _v.getBytes();
      assertEquals(i,
        Longs.fromByteArray(testChicagoCluster.chicagoClient.tsWrite(tsKey.getBytes(), val).get()));
    }
    assertTSClient(tsKey,29,"val29");

    //Test overwriting of data
    int key  = 29;
    String val  = "value29";
    //write
    long l = Longs.fromByteArray(testChicagoCluster.chicagoClient.tsWrite(tsKey.getBytes(),Longs.toByteArray(key), val.getBytes()).get());
    //Assert no overwrite took place
    assertTSClient(tsKey,key,"val29");
    testChicagoCluster.chicagoClient.deleteColFam(tsKey.getBytes());
  }

  public void assertTSClient(String colFam, int key, String val){
    List<String> nodes = testChicagoCluster.chicagoClient.getEffectiveNodes(colFam.getBytes());
    nodes.forEach(n -> {
      System.out.println("Checking node "+n);
      try {
        ChicagoAsyncClient cc = new ChicagoAsyncClient(n);
        cc.start();
        assertEquals(val,new String(cc.read(colFam.getBytes(), Longs.toByteArray(key)).get()));
      }catch (Exception e){
        e.printStackTrace();
        return;
      }
    });
  }

  @Test @Parameterized.Parameters
  public void writeCCSequence() throws Exception{
    byte[] offset = null;
    List<String> nodes=null;
    for (int i = 0; i < 30; i++) {
      String key = "key"+i;
      String _v = "val" + i;
      byte[] val = _v.getBytes();
      assertNotNull(testChicagoCluster.chicagoClient.write(key.getBytes(), val).get());
    }

    //Assert all nodes have the data
    assertCCdata("key29","val29");

    //Test overwriting of data
    String key = "key29";
    String val  = "value29";
    assertNotNull(testChicagoCluster.chicagoClient.write(key.getBytes(), val.getBytes()).get());
    //Assert overwrite is successful
    assertCCdata(key,val);
    testChicagoCluster.chicagoClient.deleteColFam(ChiUtil.defaultColFam.getBytes());
  }


  public void assertCCdata(String key,String val){
    List<String> nodes = testChicagoCluster.chicagoClient.getEffectiveNodes(ChiUtil.defaultColFam.getBytes());
    nodes.forEach(n -> {
      ChicagoAsyncClient cc = testChicagoCluster.chicagoClientHashMap.get(forServer(n));
      try {
        assertEquals(val,new String(cc.read(key.getBytes()).get()));
      }catch (Exception e){
        e.printStackTrace();
      }
    });
  }
}
