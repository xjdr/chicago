package com.xjeffrose.chicago.loadTest;

import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.curator.test.TestingServer;
import com.xjeffrose.chicago.Chicago;
import com.xjeffrose.chicago.client.ChicagoClient;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import com.xjeffrose.chicago.client.ChicagoStream;
import com.xjeffrose.chicago.client.ChicagoTSClient;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ChicagoLoadTest {
  static ChicagoTSClient chicagoClientDHT;

  @BeforeClass
  static public void setupFixture() throws Exception {
    chicagoClientDHT = new ChicagoTSClient("10.24.25.188:2181,10.24.25.189:2181,10.25.145.56:2181,10.24.33.123:2181", 3);
    //chicagoClientDHT = new ChicagoClient("10.22.100.183:2181/chicago");
  }

  @Test
  public void writeMany() throws Exception {
    String _k = "smkey";
    byte[] key = _k.getBytes();
    for (int i = 0; i < 1000; i++) {
      String _v = "val" + i;
      byte[] val = _v.getBytes();
      assertNotNull(chicagoClientDHT.write(key, val));
      System.out.println(i);
    }
  }

  @Test
  public void readStream() throws Exception {
    ListenableFuture<ChicagoStream> f = chicagoClientDHT.stream("smkey".getBytes());
    ChicagoStream cs = f.get(1000, TimeUnit.MILLISECONDS);
    ListenableFuture<byte[]> resp = cs.getStream();

    System.out.println(new String(resp.get(1000, TimeUnit.MILLISECONDS)));
  }

//  @Test
//  public void readMany() throws Exception {
//    for (int i = 0; i < 100; i++) {
//      String _k = "key" + i;
//      byte[] key = _k.getBytes();
//      String _v = "val" + i;
//      byte[] val = _v.getBytes();
//      assertEquals(new String(val), new String(chicagoClientDHT.read(key).get()));
//    }
//  }
//
//    @Test
//    public void deleteMany() throws Exception {
//      for (int i = 0; i < 100; i++) {
//        String _k = "sm" + i;
//        byte[] key = _k.getBytes();
//        assertEquals(true, chicagoClientDHT.delete(key));
//      }
//    }
}
