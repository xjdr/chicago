package com.xjeffrose.chicago.loadTest;

import com.xjeffrose.chicago.client.ChicagoClient;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by root on 5/17/16.
 */
public class ChicagoLoadTest {
  static ChicagoClient chicagoClientDHT;

  @BeforeClass
  static public void setupFixture() throws Exception {
    //chicagoClientDHT = new ChicagoClient("10.24.25.188:2181,10.24.25.189:2181,10.25.145.56:2181,10.24.33.123:2181");
    //chicagoClientDHT = new ChicagoClient("10.22.100.183:2181/chicago");
  }

  @Test
  public void writeMany() throws Exception {
    chicagoClientDHT = new ChicagoClient("10.24.25.188:2181,10.24.25.189:2181,10.25.145.56:2181,10.24.33.123:2181");
    long start_time = System.currentTimeMillis();
    for (int i = 0; i < 100; i++) {
      String _k = "key" + i;
      byte[] key = _k.getBytes();
      String _v = "val" + i;
      byte[] val = _v.getBytes();
      assertEquals(true, chicagoClientDHT.write(key, val));
    }
    long end_time = System.currentTimeMillis();
    long difference = end_time-start_time;
    System.out.println(difference);
  }

  @Test
  public void readMany() throws Exception {
    chicagoClientDHT = new ChicagoClient("10.24.25.188:2181,10.24.25.189:2181,10.25.145.56:2181,10.24.33.123:2181");
    long start_time = System.currentTimeMillis();
    for (int i = 0; i < 100; i++) {
      String _k = "key" + i;
      byte[] key = _k.getBytes();
      String _v = "val" + i;
      byte[] val = _v.getBytes();
      assertEquals(new String(val), new String(chicagoClientDHT.read(key)));
    }
    System.out.println(System.currentTimeMillis() - start_time);
  }

    @Test
    public void deleteMany() throws Exception {
      chicagoClientDHT = new ChicagoClient("10.24.25.188:2181,10.24.25.189:2181,10.25.145.56:2181,10.24.33.123:2181");
      long start_time = System.currentTimeMillis();
      for (int i = 0; i < 100; i++) {
        String _k = "key" + i;
        byte[] key = _k.getBytes();
        assertEquals(true, chicagoClientDHT.delete(key));
      }
      System.out.println(System.currentTimeMillis() - start_time);
    }
}
