package com.xjeffrose.chicago;

import com.google.common.primitives.Longs;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by root on 8/8/16.
 */
public class ChicagoClientTest {
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
  public void transactMany() throws Exception {
    for (int i = 0; i < 200; i++) {
      String _k = "xxkey" + i;
      byte[] key = _k.getBytes();
      String _v = "val" + i;
      byte[] val = _v.getBytes();
      assertNotNull(testChicagoCluster.chicagoClient.write(key, val).get());
      assertEquals(new String(val), new String(testChicagoCluster.chicagoClient.read(key).get()));
    }
  }

  @Test
  public void transactManyCF() throws Exception {
    for (int i = 0; i < 200; i++) {
      String _k = "key" + i;
      byte[] key = _k.getBytes();
      String _v = "val" + i;
      byte[] val = _v.getBytes();
      testChicagoCluster.chicagoClient.write("colfam".getBytes(), key, val);
      assertEquals(new String(val), new String(testChicagoCluster.chicagoClient.read("colfam".getBytes(), key).get()));
    }
  }

  @Test
  public void transactManyCFConcurrent() throws Exception {
    ExecutorService exe = Executors.newFixedThreadPool(6);
    int count = 500;
    CountDownLatch latch = new CountDownLatch(count * 3);


    exe.execute(new Runnable() {
      @Override
      public void run() {
        try {
          for (int i = 0; i < count; i++) {
            String _k = "xkey" + i;
            byte[] key = _k.getBytes();
            String _v = "xval" + i;
            byte[] val = _v.getBytes();
            Assert.assertNotNull(testChicagoCluster.chicagoClient.write("xcolfam".getBytes(), key, val).get());
            assertEquals(new String(val), new String(testChicagoCluster.chicagoClient.read("xcolfam".getBytes(), key).get()));
            latch.countDown();
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });


    exe.execute(new Runnable() {
      @Override
      public void run() {
        try {
          for (int i = 0; i < count; i++) {
            String _k = "ykey" + i;
            byte[] key = _k.getBytes();
            String _v = "yval" + i;
            byte[] val = _v.getBytes();
            assertNotNull(testChicagoCluster.chicagoClient.write("ycolfam".getBytes(), key, val).get());
            assertEquals(new String(val), new String(testChicagoCluster.chicagoClient.read("ycolfam".getBytes(), key).get()));
            latch.countDown();
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });


    exe.execute(new Runnable() {
      @Override
      public void run() {
        try {
          for (int i = 0; i < count; i++) {
            String _k = "zkey" + i;
            byte[] key = _k.getBytes();
            String _v = "zval" + i;
            byte[] val = _v.getBytes();
            Assert.assertNotNull(testChicagoCluster.chicagoClient.write("xcolfam".getBytes(), key, val).get());

            assertEquals(new String(val), new String(testChicagoCluster.chicagoClient.read("xcolfam".getBytes(), key).get()));
            latch.countDown();
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });


    latch.await(20000, TimeUnit.MILLISECONDS);
    exe.shutdownNow();
  }

  @Test
  public void transactStream() throws Exception {
    Set<String> values = new HashSet<>();
    Set<String> resultValues = new HashSet<>();
    String colFam = "test-colFam";
    testChicagoCluster.chicagoClient.deleteColFam(colFam.getBytes());

    for (int i = 0; i < 100; i++) {
      String _v = "val" + i + "end!!"+i;
      byte[] val = _v.getBytes();
      values.add(_v);
      byte[] ret = testChicagoCluster.chicagoClient.tsWrite(colFam.getBytes(), val).get();
      assertTrue(Longs.fromByteArray(ret) >= 0 && Longs.fromByteArray(ret) < 100);
    }

    String delimiter = ChiUtil.delimiter;

    byte[] resp = testChicagoCluster.chicagoClient.stream(colFam
      .getBytes(),Longs.toByteArray(0l)).get();

    assertNotNull(resp);
    byte[] resultArray = resp;
    long offset;
    String result = new String(resultArray);

    long old = -1;
    int count = 0;
    while (result.contains(delimiter)) {

      if (count > 10) {
        break;
      }
      offset = ChiUtil.findOffset(resultArray);
      if (old != -1 && (old == offset)) {
        Thread.sleep(500);
      }


      String output = result.split(ChiUtil.delimiter)[0];
      for(String line : output.split("\0")){
        resultValues.add(line);
      }

      resp = testChicagoCluster.chicagoClient.stream(
        colFam.getBytes(), Longs.toByteArray(offset)).get();
      resultArray = resp;
      result = new String(resp);
      old = offset;
      count++;
    }

    values.removeAll(resultValues);
    assertTrue(values.isEmpty());
    testChicagoCluster.chicagoClient.deleteColFam(colFam.getBytes());
  }

  @Test
  public void transactLargeStream() throws Exception {
    byte[] value=null;
    String returnValString="someJunk";
    String colFam = "LargeTskey";
    int size=1024;
    testChicagoCluster.chicagoClient.deleteColFam(colFam.getBytes());

    byte[] offset = null;
    for (int i = 0; i < 50; i++) {
      byte[] val = new byte[size];
      if (i == 12) {
        for(int j =0 ; j< size ; j++){
          val[j] = 10;
        }
        value = val;
        offset = testChicagoCluster.chicagoClient.tsWrite(colFam.getBytes(), val).get();
      }else {
        assertNotNull(
          testChicagoCluster.chicagoClient.tsWrite(colFam.getBytes(), val).get());
      }
    }

    byte[] _resp = testChicagoCluster.chicagoClient.stream(
      colFam.getBytes(), offset).get();
    String result = new String(_resp);
    assertTrue(result.contains(ChiUtil.delimiter));
    returnValString = result.split(ChiUtil.delimiter)[0].split("\0")[0];
    assertEquals(new String(value), returnValString);
    testChicagoCluster.chicagoClient.deleteColFam(colFam.getBytes());
  }

}
