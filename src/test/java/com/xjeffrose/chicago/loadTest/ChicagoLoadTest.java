package com.xjeffrose.chicago.loadTest;

import com.netflix.curator.test.TestingServer;
import com.xjeffrose.chicago.Chicago;
import com.xjeffrose.chicago.client.ChicagoClient;
import java.net.InetSocketAddress;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ChicagoLoadTest {
  static ChicagoClient chicagoClientDHT;

  @BeforeClass
  static public void setupFixture() throws Exception {
    chicagoClientDHT = new ChicagoClient("10.24.25.188:2181,10.24.25.189:2181,10.25.145.56:2181,10.24.33.123:2181", 3);
    //chicagoClientDHT = new ChicagoClient("10.22.100.183:2181/chicago");
  }

  //@Test
  public void writeMany() throws Exception {
    for (int i = 0; i < 100; i++) {
      String _k = "key" + i;
      byte[] key = _k.getBytes();
      String _v = "val" + i;
      byte[] val = _v.getBytes();
      assertEquals(true, chicagoClientDHT.write(key, val));
    }
  }

  //@Test
  public void readMany() throws Exception {
    for (int i = 0; i < 100; i++) {
      String _k = "key" + i;
      byte[] key = _k.getBytes();
      String _v = "val" + i;
      byte[] val = _v.getBytes();
      assertEquals(new String(val), new String(chicagoClientDHT.read(key).get()));
    }
  }

  //@Test
    public void deleteMany() throws Exception {
      for (int i = 0; i < 100; i++) {
        String _k = "sm" + i;
        byte[] key = _k.getBytes();
        assertEquals(true, chicagoClientDHT.delete(key));
      }
    }
}
