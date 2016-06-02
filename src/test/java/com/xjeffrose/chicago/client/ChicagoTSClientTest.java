package com.xjeffrose.chicago.client;

import com.xjeffrose.chicago.TestChicago;
import com.xjeffrose.chicago.server.ChicagoServer;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.curator.test.TestingServer;
import com.xjeffrose.chicago.Chicago;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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

    chicagoTSClient = new ChicagoTSClient(testingServer.getConnectString(), 3);
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
  public void transactStream() throws Exception {
    byte[] offset = null;
    for (int i = 0; i < 2000; i++) {
      String _v = "val" + i;
      byte[] val = _v.getBytes();
      if (i == 12) {
        offset = chicagoTSClient.write("tskey".getBytes(), val);
      }
      assertNotNull(chicagoTSClient.write("tskey".getBytes(), val));
    }

    ListenableFuture<ChicagoStream> f = chicagoTSClient.stream("tskey".getBytes());
    ChicagoStream cs = f.get(1000, TimeUnit.MILLISECONDS);
    ListenableFuture<byte[]> resp = cs.getStream();

    assertNotNull(resp.get(1000, TimeUnit.MILLISECONDS));

    ListenableFuture<ChicagoStream> _f = chicagoTSClient.stream("tskey".getBytes(), offset);
    ChicagoStream _cs = _f.get(1000, TimeUnit.MILLISECONDS);
    ListenableFuture<byte[]> _resp = _cs.getStream();

    assertNotNull(_resp.get(1000, TimeUnit.MILLISECONDS));
  }


  @Test
  public void transactLargeStream() throws Exception {
    byte[] offset = null;
    for (int i = 0; i < 1; i++) {
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
