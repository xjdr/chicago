package com.xjeffrose.chicago.client;

import com.xjeffrose.chicago.TestChicago;
import com.xjeffrose.chicago.server.ChicagoServer;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.curator.test.TestingServer;

import java.util.Arrays;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.junit.Assert.*;


public class ChicagoTSClientTest {
  TestingServer testingServer;
  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();
  List<ChicagoServer> servers;
  ChicagoTSClient chicagoTSClient;

  @Before
  public void setup() throws Exception {
    testingServer = new TestingServer(true);
    servers = TestChicago.makeServers(TestChicago.chicago_dir(tmp), 4, testingServer.getConnectString());
    for (ChicagoServer server : servers) {
      server.start();
    }

    chicagoTSClient = new ChicagoTSClient(testingServer.getConnectString(), 4);
    chicagoTSClient.startAndWaitForNodes(4);
    //chicagoTSClient = new ChicagoTSClient("localhost:2181", 1);
    //chicagoTSClient.startAndWaitForNodes(1);
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
  public void transactStream() throws Exception {
    byte[] offset = null;
    for (int i = 0; i < 1000; i++) {
      String _v = "val" + i;
      byte[] val = _v.getBytes();
      if (i == 12) {
        offset = chicagoTSClient.write("tskey".getBytes(), val);
      }
      assertNotNull(chicagoTSClient.write("tskey".getBytes(), val));
      //System.out.println(i);
    }

    ListenableFuture<ChicagoStream> f = chicagoTSClient.stream("tskey".getBytes());
    ChicagoStream cs = f.get();
    ListenableFuture<byte[]> resp = cs.getStream();

    assertNotNull(resp.get());
    String result = new String(resp.get());
    byte[] old=null;
    int count =0;
    while(result.contains("@@@")){

      if(count > 10){
        break;
      }
      offset = result.split("@@@")[1].getBytes();
      if(old != null && Arrays.equals(old,offset)){
        Thread.sleep(500);
      }
      System.out.println(result.split("@@@")[0]);
      cs.close();
      ListenableFuture<ChicagoStream> _f = chicagoTSClient.stream("tskey".getBytes(), offset);
      cs = _f.get();
      resp = cs.getStream();
      result = new String(resp.get());
      old = offset;
      count++;
    }

    ListenableFuture<ChicagoStream> _f = chicagoTSClient.stream("tskey".getBytes(), offset);
    cs = _f.get();
    ListenableFuture<byte[]> _resp = cs.getStream();

    assertNotNull(_resp.get());
    System.out.println(new String(_resp.get()));
  }


  @Test
  public void transactLargeStream() throws Exception {
    byte[] offset = null;
    for (int i = 0; i < 10; i++) {
      byte[] val = new byte[10240];
      if (i == 12) {
        offset = chicagoTSClient.write("LargeTskey".getBytes(), val);
      }
      assertNotNull(chicagoTSClient.write("tskey".getBytes(), val));
    }

    ListenableFuture<byte[]> f = chicagoTSClient.read("tskey".getBytes());
    byte[] resp = f.get();

    System.out.println(new String(resp));

    ListenableFuture<ChicagoStream> _f = chicagoTSClient.stream("tskey".getBytes(), offset);
    ChicagoStream _cs = _f.get();
    ListenableFuture<byte[]> _resp = _cs.getStream();

    System.out.println(new String(_resp.get()));
  }
}
