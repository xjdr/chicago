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
  static ChicagoClient chicagoClientDHT;

  @BeforeClass
  static public void setupFixture() throws Exception {
    chicagoClientDHT = new ChicagoClient("127.0.0.1:2181", 1);
    //chicagoClientDHT = new ChicagoClient("10.22.100.183:2181/chicago");
  }

  @Test
  public void writeMany() throws Exception {
    StringBuilder _v = new StringBuilder("");
    for (int i = 1; i < 1000000; i++) {
      String _k = "key" + i;
      byte[] key = _k.getBytes();
      if(i%1000 ==0){
        _v.delete(0,_v.length()-1);
      }
      _v.append("val" + i);
      byte[] val = _v.toString().getBytes();
      assertEquals(true,chicagoClientDHT.write(key, val));
      //assertEquals(new String(val), new String(chicagoClientDHT.read(key).get()));
      System.out.println(i);
    }
  }

  //@Test
  //public void readStream() throws Exception {
  //  ListenableFuture<ChicagoStream> f = chicagoClientDHT.stream("smkey".getBytes());
  //  ChicagoStream cs = f.get(1000, TimeUnit.MILLISECONDS);
  //  ListenableFuture<byte[]> resp = cs.getStream();
  //
  //  System.out.println(new String(resp.get(1000, TimeUnit.MILLISECONDS)));
  //}

  @Test
  public void readMany() throws Exception {
    StringBuilder _v = new StringBuilder("");
    for (int i = 98001; i < 99000; i++) {
      String _k = "key" + i;
      byte[] key = _k.getBytes();
      if(i%1000 ==0){
        _v.delete(0,_v.length()-1);
      }
      _v.append("val" + i);
      byte[] val = _v.toString().getBytes();
      assertEquals(_v.toString(),new String(chicagoClientDHT.read(key).get()));
    }
  }
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
