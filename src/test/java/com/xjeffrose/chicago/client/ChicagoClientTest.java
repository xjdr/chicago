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


public class ChicagoClientTest {
  TestingServer testingServer;
  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();
  List<ChicagoServer> servers;
  ChicagoClient chicagoClientDHT;

  @Before
  public void setup() throws Exception {
    testingServer = new TestingServer(true);
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
  public void transactOnce() throws Exception {
    for (int i = 0; i < 1; i++) {
      String _k = "key" + i;
      byte[] key = _k.getBytes();
      String _v = "val" + i;
      byte[] val = _v.getBytes();
      assertEquals(true, chicagoClientDHT.write(key, val));
      assertEquals(new String(val), new String(chicagoClientDHT.read(key).get()));
      assertEquals(true, chicagoClientDHT.delete(key));
    }
    int transactionSum = 0;
    for (ChicagoServer server : servers) {
      transactionSum += server.dbLog.entries.size();
    }
    assertEquals(1*3*3, transactionSum);
  }

  @Test
  public void transactMany() throws Exception {
    for (int i = 0; i < 2000; i++) {
      String _k = "key" + i;
      byte[] key = _k.getBytes();
      String _v = "val" + i;
      byte[] val = _v.getBytes();
      assertEquals(true, chicagoClientDHT.write(key, val));
      assertEquals(new String(val), new String(chicagoClientDHT.read(key).get()));
      assertEquals(true, chicagoClientDHT.delete(key));
    }
    int transactionSum = 0;
    for (ChicagoServer server : servers) {
      transactionSum += server.dbLog.entries.size();
    }
    assertEquals(2000*3*3, transactionSum);
  }

  @Test
  public void transactManyCF() throws Exception {
    for (int i = 0; i < 2000; i++) {
      String _k = "key" + i;
      byte[] key = _k.getBytes();
      String _v = "val" + i;
      byte[] val = _v.getBytes();
      assertEquals(true, chicagoClientDHT.write("colfam".getBytes(), key, val));
      assertEquals(new String(val), new String(chicagoClientDHT.read("colfam".getBytes(), key).get()));
      assertEquals(true, chicagoClientDHT.delete("colfam".getBytes(), key));
    }
  }

  @Test
  public void transactManyCFConcurrent() throws Exception {
    ExecutorService exe = Executors.newFixedThreadPool(6);
    int count = 2000;
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
            assertEquals(true, chicagoClientDHT.write("xcolfam".getBytes(), key, val));
            assertEquals(new String(val), new String(chicagoClientDHT.read("xcolfam".getBytes(), key).get()));
            assertEquals(true, chicagoClientDHT.delete("xcolfam".getBytes(), key));
//            System.out.println("2 " + latch.getCount());
            latch.countDown();
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
            assertEquals(true, chicagoClientDHT.write("ycolfam".getBytes(), key, val));
            assertEquals(new String(val), new String(chicagoClientDHT.read("ycolfam".getBytes(), key).get()));
            assertEquals(true, chicagoClientDHT.delete("ycolfam".getBytes(), key));
//            System.out.println("1 " + latch.getCount());
            latch.countDown();
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
            assertEquals(true, chicagoClientDHT.write("xcolfam".getBytes(), key, val));
            assertEquals(new String(val), new String(chicagoClientDHT.read("xcolfam".getBytes(), key).get()));
            assertEquals(true, chicagoClientDHT.delete("xcolfam".getBytes(), key));
//            System.out.println("2 " + latch.getCount());
            latch.countDown();
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
    });


    latch.await(20000, TimeUnit.MILLISECONDS);
  }
}
