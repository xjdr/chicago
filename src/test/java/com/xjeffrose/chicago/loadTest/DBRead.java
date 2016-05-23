package com.xjeffrose.chicago.loadTest;


import com.google.common.hash.Funnels;
import com.netflix.curator.test.TestingServer;
import com.xjeffrose.chicago.Chicago;
import com.xjeffrose.chicago.client.ChicagoClient;
import java.net.InetSocketAddress;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DBRead {

    static ChicagoClient chicagoClientDHT;
    public static final int MAX_KEY=17300;
    public static final int MAX_VAL=17300;
    static TestingServer testingServer;
    static Chicago chicago1;
    static Chicago chicago2;
    static Chicago chicago3;
    static Chicago chicago4;

  @BeforeClass
  static public void setupFixture() throws Exception {
    //chicagoClientDHT = new ChicagoClient("10.24.25.188:2181,10.24.25.189:2181,10.25.145.56:2181,10.24.33.123:2181");
    //chicagoClientDHT = new ChicagoClient("10.22.100.183:2181/chicago");
    //chicagoClientDHT = new ChicagoClient("10.24.25.188:2181,10.24.25.189:2181,10.25.145.56:2181,10.24.33.123:2181");
      testingServer = new TestingServer(2182);
      chicago1 = new Chicago();
      chicago1.main(new String[]{"", "src/test/resources/test1.conf"});
      chicago2 = new Chicago();
      chicago2.main(new String[]{"", "src/test/resources/test2.conf"});
      chicago3 = new Chicago();
      chicago3.main(new String[]{"", "src/test/resources/test3.conf"});
      chicago4 = new Chicago();
      chicago4.main(new String[]{"", "src/test/resources/test4.conf"});
      chicagoClientDHT = new ChicagoClient("127.0.0.1:2182");
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

    @Test
    public void verifyDeleteData() throws Exception {
        long start_time = System.currentTimeMillis();
        String _k ="smk";
        byte[] key = _k.getBytes();
        String _v = "vall";
        byte[] val = _v.getBytes();
        List<String> l = chicagoClientDHT.getNodeListforKey(key);
        System.out.println(l.toString());
        assertEquals(true, chicagoClientDHT.write(key, val));
        switch(l.get(0)){
            case "127.0.0.1":
                System.out.println("Stopping chicago1");
                chicago1.stop();
                break;
            case "127.0.0.2":
                System.out.println("Stopping chicago2");
                chicago2.stop();
                break;
            case "127.0.0.3":
                System.out.println("Stopping chicago3");
                chicago3.stop();
                break;
            case "127.0.0.4":
                System.out.println("Stopping chicago4");
                chicago4.stop();
                break;
        }
        l = chicagoClientDHT.getNodeListforKey(key);
        System.out.println(l.toString());
        assertEquals(true, chicagoClientDHT.delete(key));
        long diff = System.currentTimeMillis() - start_time;
    }
}
