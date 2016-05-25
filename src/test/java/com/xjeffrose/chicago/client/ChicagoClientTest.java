package com.xjeffrose.chicago.client;

import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.curator.test.TestingServer;
import com.xjeffrose.chicago.Chicago;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ChicagoClientTest {
  static TestingServer testingServer;
  static Chicago chicago1;
  static Chicago chicago2;
  static Chicago chicago3;
  static Chicago chicago4;

  static ChicagoClient chicagoClientSingle;
  static ChicagoClient chicagoClientDHT;
  static ChicagoTSClient chicagoTSClient;


  @BeforeClass
  static public void setupFixture() throws Exception {
    testingServer = new TestingServer(2182);
    chicago1 = new Chicago();
    chicago1.main(new String[]{"", "src/test/resources/test1.conf"});
    chicago2 = new Chicago();
    chicago2.main(new String[]{"", "src/test/resources/test2.conf"});
    chicago3 = new Chicago();
    chicago3.main(new String[]{"", "src/test/resources/test3.conf"});
    chicago4 = new Chicago();
    chicago4.main(new String[]{"", "src/test/resources/test4.conf"});
//    chicagoClientSingle = new ChicagoClient(new InetSocketAddress("127.0.0.1", 12000));
    chicagoClientDHT = new ChicagoClient("10.25.160.234:2181", 3);
//    chicagoTSClient = new ChicagoTSClient("10.25.160.234:2181", 3);
    chicagoTSClient = new ChicagoTSClient(testingServer.getConnectString(), 3);


//    chicagoClientDHT = new ChicagoClient("10.22.100.183:2181");
//    chicagoClientDHT = new ChicagoClient(testingServer.getConnectString());
//    chicagoClientDHT = new ChicagoClient("10.24.25.188:2181,10.24.25.189:2181,10.25.145.56:2181,10.24.33.123:2181");
//    chicagoClientDHT = new ChicagoClient("10.22.100.183:2181,10.25.180.234:2181,10.22.103.86:2181,10.25.180.247:2181,10.25.69.226:2181/chicago");
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
  }

  @Test
  public void transactManyCF() throws Exception {
    for (int i = 0; i < 20; i++) {
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
  public void transactStream() throws Exception {
    byte[] offset = null;
    for (int i = 0; i < 20; i++) {
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

//    System.out.println(new String(resp.get(1000, TimeUnit.MILLISECONDS)));
    assertEquals("val0\u0000val1\u0000val2\u0000val3\u0000val4\u0000val5\u0000val6\u0000val7\u0000val8\u0000val9\u0000val10\u0000val11\u0000val12\u0000val12\u0000val13\u0000val14\u0000val15\u0000val16\u0000val17\u0000val18\u0000val19\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000", new String(resp.get(1000, TimeUnit.MILLISECONDS)));


    ListenableFuture<ChicagoStream> _f = chicagoTSClient.stream("tskey".getBytes(), offset);
    ChicagoStream _cs = _f.get(1000, TimeUnit.MILLISECONDS);
    ListenableFuture<byte[]> _resp = _cs.getStream();

//    System.out.println(new String(_resp.get(1000, TimeUnit.MILLISECONDS)));
    assertEquals("val12\u0000val12\u0000val13\u0000val14\u0000val15\u0000val16\u0000val17\u0000val18\u0000val19\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000", new String(_resp.get(1000, TimeUnit.MILLISECONDS)));
  }

  @Test
  public void transactManyCFConcurrent() throws Exception {
    ExecutorService exe = Executors.newFixedThreadPool(4);
    int count = 4;
    CountDownLatch latch = new CountDownLatch(count * 2);


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
            System.out.println("1 " + latch.getCount());
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
            String _k = "xkey" + i;
            byte[] key = _k.getBytes();
            String _v = "xval" + i;
            byte[] val = _v.getBytes();
            assertEquals(true, chicagoClientDHT.write("xcolfam".getBytes(), key, val));
            assertEquals(new String(val), new String(chicagoClientDHT.read("xcolfam".getBytes(), key).get()));
            assertEquals(true, chicagoClientDHT.delete("xcolfam".getBytes(), key));
            System.out.println("2 " + latch.getCount());
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

    while(true) {
      Thread.sleep(10000);
    }
//    latch.await(10000, TimeUnit.MILLISECONDS);
  }

}