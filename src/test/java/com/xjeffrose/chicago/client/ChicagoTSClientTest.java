package com.xjeffrose.chicago.client;

import lombok.extern.slf4j.Slf4j;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import com.google.common.primitives.Longs;
import com.xjeffrose.chicago.ChiUtil;

import com.xjeffrose.chicago.TestChicago;
import java.util.concurrent.ExecutionException;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.util.concurrent.ListenableFuture;
import com.xjeffrose.chicago.server.ChicagoServer;

@Slf4j
public class ChicagoTSClientTest {
  TestingServer testingServer;
  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();
  List<ChicagoServer> servers;
  ChicagoClient chicagoTSClient;

  @Before
  public void setup() throws Exception {
    InstanceSpec spec = new InstanceSpec(null, -1, -1, -1, true, -1,
        2000, -1);
    testingServer = new TestingServer(spec, true);
    servers = TestChicago.makeServers(TestChicago.chicago_dir(tmp), 4,
        testingServer.getConnectString());
    for (ChicagoServer server : servers) {
      server.start();
    }
    chicagoTSClient = new ChicagoClient(testingServer.getConnectString(),
        3);
    chicagoTSClient.startAndWaitForNodes(4);
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
  public void transactStreamWhileWritingSameClient() throws Exception {
    for (int i = 0; i < 10; i++) {
      String _v = "val" + i;
      byte[] val = _v.getBytes();
      assertNotNull(chicagoTSClient.tsWrite("tskey".getBytes(), val).get().get(0));
      // System.out.println(i);
    }
    final ChicagoClient chicagoTSClientParalellel = new ChicagoClient(
        testingServer.getConnectString(), 3);
    chicagoTSClientParalellel.startAndWaitForNodes(4);
    final ArrayList<String> response = new ArrayList<String>();
    Runnable runnableStreamer = new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(200);
          ListenableFuture<List<byte[]>> _f = chicagoTSClientParalellel
              .stream("tskey".getBytes(), Longs.toByteArray(2));
          assertNotNull(new String(_f.get().get(0)));
          response.add(new String(_f.get().get(0)));
          Thread.sleep(200);
          _f = chicagoTSClientParalellel.stream("tskey".getBytes(),
              Longs.toByteArray(4));
          assertNotNull(new String(_f.get().get(0)));
          response.add(new String(_f.get().get(0)));
          Thread.sleep(200);
          _f = chicagoTSClientParalellel.stream("tskey".getBytes(),
              Longs.toByteArray(6));
          assertNotNull(new String(_f.get().get(0)));
          response.add(new String(_f.get().get(0)));
        } catch (Exception e) {
        }
      }
    };

    Runnable runnable2 = new Runnable() {
      @Override
      public void run() {
        for (int i = 10; i < 20; i++) {
          String _v = "val" + i;
          byte[] val = _v.getBytes();
          try {
            chicagoTSClientParalellel
                .tsWrite("tskey".getBytes(), val);
            try {
              Thread.sleep(200);
            } catch (InterruptedException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
          } catch (ChicagoClientTimeoutException e) {
            e.printStackTrace();
          } catch (ChicagoClientException e) {
            e.printStackTrace();
          }
        }
      }
    };

    Thread t1 = new Thread(runnableStreamer);
    Thread t2 = new Thread(runnable2);

    t1.start();
    t2.start();
    t1.join();
    t2.join();
    Assert.assertEquals(response.size(), 3);
    log.debug(response.get(0) + " /// " + response.get(1)
        + response.get(2));
    Assert.assertEquals(response.get(0).contains("val11"), false);// Ensures
                                    // that
                                    // insert
                                    // and
                                    // read
                                    // are
                                    // interwoven
    Assert.assertEquals(response.get(2).contains("val11"), true);// Ensures
                                    // that
                                    // insert
                                    // and
                                    // read
                                    // are
                                    // interwoven

  }


  @Test
  public void transactStreamWhileWritingDifferentClients() throws Exception {
    for (int i = 0; i < 10; i++) {
      String _v = "val" + i;
      byte[] val = _v.getBytes();
      assertNotNull(chicagoTSClient.tsWrite("tskey".getBytes(), val));
      // System.out.println(i);
    }
    final ChicagoClient chicagoTSClientParalellel = new ChicagoClient(
        testingServer.getConnectString(), 3);
    chicagoTSClientParalellel.startAndWaitForNodes(4);
    final ArrayList<String> response = new ArrayList<String>();
    Runnable runnableStreamer = new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(200);
          byte[] _resp = chicagoTSClient
            .stream("tskey".getBytes(), Longs.toByteArray(2)).get().get(0);
          assertNotNull(_resp);
          response.add(new String(_resp));
          Thread.sleep(200);
          _resp = chicagoTSClient.stream("tskey".getBytes(),
            Longs.toByteArray(4)).get().get(0);
          assertNotNull(_resp);
          response.add(new String(_resp));
          Thread.sleep(200);
          _resp = chicagoTSClient.stream("tskey".getBytes(),
            Longs.toByteArray(6)).get().get(0);
          assertNotNull(_resp);
          response.add(new String(_resp));
        } catch (Exception e) {

        }
      }
    };

    Runnable runnable2 = new Runnable() {
      @Override
      public void run() {
        for (int i = 10; i < 20; i++) {
          String _v = "val" + i;
          byte[] val = _v.getBytes();
          try {
            chicagoTSClientParalellel
                .tsWrite("tskey".getBytes(), val).get().get(0);
            try {
              Thread.sleep(200);
            } catch (InterruptedException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
          } catch (ChicagoClientTimeoutException e) {
            e.printStackTrace();
          } catch (ChicagoClientException e) {
            e.printStackTrace();
          } catch (InterruptedException e) {
            e.printStackTrace();
          } catch (ExecutionException e) {
            e.printStackTrace();
          }
        }
      }
    };

    Thread t1 = new Thread(runnableStreamer);
    Thread t2 = new Thread(runnable2);

    t1.start();
    t2.start();
    t1.join();
    t2.join();
    Assert.assertEquals(response.size(), 3);
    log.debug(response.get(0) + " " + response.get(1)
        + response.get(2));
    Assert.assertEquals(response.get(0).contains("val11"), false);// Ensures
                                    // that
                                    // insert
                                    // and
                                    // read
                                    // are
                                    // interwoven
    Assert.assertEquals(response.get(2).contains("val11"), true);// Ensures
                                    // that
                                    // insert
                                    // and
                                    // read
                                    // are
                                    // interwoven

  }

  @Test
  public void transactRoundTripStream() throws Exception {
    long startTime=System.currentTimeMillis();
    for (int i = 0; (i < 1000); i++) {
      String _v = "val" + i;
      byte[] val = _v.getBytes();
      assertNotNull(chicagoTSClient.tsWrite("tskey".getBytes(), val).get().get(0));
      boolean gotResponse = false;
      while (!gotResponse) {
        byte[] _resp = chicagoTSClient.stream(
          "tskey".getBytes(), Longs.toByteArray(i)).get().get(0);
        assertNotNull(_resp);
        String response = new String(_resp);
        if (response.contains(_v)) {
          gotResponse = true;
        }
      }
    }
    long testTime=(System.currentTimeMillis()-startTime)/1000;
    log.debug("Time taken for one round robin read/write on average is: "+testTime );
    Assert.assertTrue(testTime <10);
  }

  @Test
  public void transactStream() throws Exception {

    for (int i = 0; i < 1000; i++) {
      String _v = "val" + i + "                                                   " +
        "                                                                          " +
        "                                                                    end!!"+i;
      byte[] val = _v.getBytes();
      assertNotNull(chicagoTSClient.tsWrite("tskey".getBytes(), val));
      // System.out.println(i);
    }

    String delimiter = ChiUtil.delimiter;

    byte[] resp = chicagoTSClient.stream("tskey"
      .getBytes()).get().get(0);

    assertNotNull(resp);
    byte[] resultArray = resp;
    long offset = ChiUtil.findOffset(resultArray);
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
      log.debug(output);

      if(!output.isEmpty()){
        offset = offset +1;
      }
      resp = chicagoTSClient.stream(
        "tskey".getBytes(), Longs.toByteArray(offset)).get().get(0);
      resultArray = resp;
      result = new String(resp);
      old = offset;
      count++;
    }

    byte[] _resp = chicagoTSClient.stream(
      "tskey".getBytes(), Longs.toByteArray(offset)).get().get(0);

    assertNotNull(_resp);
    log.debug(new String(_resp));
  }

  @Test
  public void transactLargeStream() throws Exception {
    byte[] offset = null;
    for (int i = 0; i < 10; i++) {
      byte[] val = new byte[10240];
      if (i == 12) {
        offset = chicagoTSClient.tsWrite("LargeTskey".getBytes(), val).get().get(0);
      }
      assertNotNull(chicagoTSClient.tsWrite("LargeTskey".getBytes(), val).get().get(0));
    }

    byte[] resp = chicagoTSClient.stream("LargeTskey".getBytes()).get().get(0);

    log.debug(new String(resp));
    byte[] _resp = chicagoTSClient.stream(
      "tskey".getBytes(), offset).get().get(0);

    log.debug(new String(_resp));
  }
}
