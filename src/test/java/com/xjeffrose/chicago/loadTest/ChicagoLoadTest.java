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
  public final static int count =1000;

  @BeforeClass
  static public void setupFixture() throws Exception {
    //chicagoClientDHT = new ChicagoClient("10.24.25.188:2181,10.24.25.189:2181,10.25.145.56:2181,10.24.33.123:2181");
    chicagoClientDHT =  new ChicagoClient("10.22.100.183:2181,10.25.180.234:2181,10.22.103.86:2181,10.25.180.247:2181,10.25.69.226:2181/chicago");
    //chicagoClientDHT = new ChicagoClient("10.22.100.183:2181/chicago");
  }

  @Test
  public void writeMany() throws Exception {
    long start_time = System.currentTimeMillis();
    for (int i = 0; i < count; i++) {
      String _k = "key" + i;
      byte[] key = _k.getBytes();
      String _v = "val" + i;
      byte[] val = _v.getBytes();
      assertEquals(true, chicagoClientDHT.write(key, val));
    }
    long diff = System.currentTimeMillis() - start_time;
    System.out.println("total time = " + diff);
    System.out.println("Avg per write = " + ((float)diff/count));
  }

  @Test
  public void readMany() throws Exception {
    long start_time = System.currentTimeMillis();
    for (int i = 0; i < count; i++) {
      String _k = "key" + i;
      byte[] key = _k.getBytes();
      String _v = "val" + i;
      byte[] val = _v.getBytes();
      assertEquals(new String(val), new String(chicagoClientDHT.read(key)));
    }
    long diff = System.currentTimeMillis() - start_time;
    System.out.println("total time = " + diff);
    System.out.println("Avg per read = " + ((float)diff/count));
  }

    @Test
    public void deleteMany() throws Exception {
      long start_time = System.currentTimeMillis();
      for (int i = 0; i < count; i++) {
        String _k = "key" + i;
        byte[] key = _k.getBytes();
        assertEquals(true, chicagoClientDHT.delete(key));
      }
      long diff = System.currentTimeMillis() - start_time;
      System.out.println("total time = " + diff);
      System.out.println("Avg per delete = " + ((float)diff/count));
    }
}
