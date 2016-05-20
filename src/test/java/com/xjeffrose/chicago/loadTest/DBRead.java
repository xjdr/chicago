package com.xjeffrose.chicago.loadTest;


import com.google.common.hash.Funnels;
import com.xjeffrose.chicago.client.ChicagoClient;
import java.net.InetSocketAddress;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by root on 5/17/16.
 */
public class DBRead {
  @Test
  public void verifyData() throws Exception {
    ChicagoClient chicagoClientDHT = new ChicagoClient(
        "10.24.25.188:2181,10.24.25.189:2181,10.25.145.56:2181,10.24.33.123:2181");
    String _k = "key20";
    byte[] key = _k.getBytes();
    String _v = "val";
    byte[] val = _v.getBytes();
    assertEquals(true, chicagoClientDHT.write(key, val));
    //assertEquals("val2", new String(chicagoClientDHT.read(key)));
    System.out.println("On 10.24.25.188, value = " + getData("10.24.25.188", _k));
    System.out.println("On 10.25.145.56, value = " + getData("10.25.145.56", _k));
    System.out.println("On 10.24.25.189, value = " + getData("10.24.25.189", _k));
    System.out.println("On 10.24.33.123, value = " + getData("10.24.33.123", _k));
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
