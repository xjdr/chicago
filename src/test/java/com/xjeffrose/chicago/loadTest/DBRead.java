package com.xjeffrose.chicago.loadTest;


import com.google.common.hash.Funnels;
import com.xjeffrose.chicago.client.ChicagoClient;
import java.net.InetSocketAddress;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by root on 5/17/16.
 */
public class DBRead {

  static ChicagoClient chicagoClientDHT;
  public static final int MAX_KEY=17300;
  public static final int MAX_VAL=17300;

  @BeforeClass
  static public void setupFixture() throws Exception {
    //chicagoClientDHT = new ChicagoClient("10.24.25.188:2181,10.24.25.189:2181,10.25.145.56:2181,10.24.33.123:2181");
    //chicagoClientDHT = new ChicagoClient("10.22.100.183:2181/chicago");
    chicagoClientDHT = new ChicagoClient(
            "10.24.25.188:2181,10.24.25.189:2181,10.25.145.56:2181,10.24.33.123:2181");
  }

  @Test
  public void verifyData() throws Exception {

    StringBuilder keystr = new StringBuilder();
    StringBuilder valstr = new StringBuilder();
    long start_time = System.currentTimeMillis();
    //String _k = keystr.append("KEY_APPEND").append(System.currentTimeMillis() / 1000L).toString();
    String _k ="smk";
    byte[] key = _k.getBytes();
    String _v = "vall";
    byte[] val = _v.getBytes();
    assertEquals(true, chicagoClientDHT.write(key, val));
    long diff = System.currentTimeMillis() - start_time;
    System.out.println("total time = " + diff);
    System.out.println("On 10.24.25.188, value = " + getData("10.24.25.188", _k));
    System.out.println("On 10.25.145.56, value = " + getData("10.25.145.56", _k));
    //System.out.println("On 10.24.25.189, value = " + getData("10.24.25.189", _k));
    System.out.println("On 10.24.33.123, value = " + getData("10.24.33.123", _k));
  }

  @Test
  public void maxKeyTest() throws Exception {
    ChicagoClient chicagoClientDHT = new ChicagoClient(
            "10.24.25.188:2181,10.24.25.189:2181,10.25.145.56:2181,10.24.33.123:2181");
    StringBuilder keystr = new StringBuilder();
    StringBuilder valstr = new StringBuilder();
    for(int i =0;i<1000;i++) {
      String _k = keystr.append("KEY_APPEND").append(System.currentTimeMillis() / 1000L).toString();
      //String _k ="smk"+i;
      byte[] key = _k.getBytes();
      //String _v = valstr.append("VAL_APPEND").append(System.currentTimeMillis() / 1000L).toString();
      String _v = "vall"+i;
      byte[] val = _v.getBytes();
      System.out.println(i +": "+ _k);
      System.out.println(_k.length());
      if(_k.length() < MAX_KEY) {
        assertEquals(true, chicagoClientDHT.write(key, val));
        assertEquals(_v, new String(chicagoClientDHT.read(key)));
      }
    }
  }


  @Test
  public void maxValueTest() throws Exception {
    StringBuilder keystr = new StringBuilder();
    StringBuilder valstr = new StringBuilder();
    long start_time = System.currentTimeMillis();
    for(int i =0;i<1000;i++) {
      String _k ="smk"+i;
      byte[] key = _k.getBytes();
      String _v = valstr.append("VAL_APPEND").append(System.currentTimeMillis() / 1000L).toString();
      byte[] val = _v.getBytes();
      System.out.println("Key = "+_k+ " "+ i +": "+ _v);
      System.out.println(_v.length());
      if(_v.length() < MAX_VAL) {
        assertEquals(true, chicagoClientDHT.write(key, val));
        assertEquals(_v, new String(chicagoClientDHT.read(key)));
      }
    }
    long diff = System.currentTimeMillis() - start_time;
    System.out.println("total time = " + diff);
  }

  public String getData(String hostname, String key){
    ChicagoClient cc = new ChicagoClient(new InetSocketAddress(hostname, 12000));
    byte[] ret=null;
    try {
      ret = cc.read(key.getBytes());
    }catch (Exception e){
      System.out.println(e.getCause());
    }
    String result = (ret == null)?"":new String(ret);
    return result;
  }

}
