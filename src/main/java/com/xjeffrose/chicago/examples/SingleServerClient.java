package com.xjeffrose.chicago.examples;

import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ListenableFuture;
import com.xjeffrose.chicago.client.*;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Created by root on 6/10/16.
 */
public class SingleServerClient {

  public static void main(String[] args) throws InterruptedException, ChicagoClientTimeoutException, ExecutionException, ChicagoClientException, Exception {
    //ChicagoClient cc = new ChicagoClient("10.24.25.188:2181,10.24.25.189:2181,10.24.33.123:2181,10.25.145.56:2181",3);
    //cc.startAndWaitForNodes(3);
//    cc.deleteColFam("tsRepKey".getBytes()).get();
    //write1000("127.0.0.1:12000");
     //write("127.0.0.1:12000","tskey",34,"val34");
    //printStream("10.22.100.183:12000", "ppfe");
    //getValue("10.24.33.123:12000", "writeTestKey",496);
    //getValue("10.24.25.189:12000", "writeTestKey",496);
    //getValue("10.25.145.56:12000", "writeTestKey",496);
    //printStream("10.24.25.189:12000");
     //printStream("10.25.145.56:12000");
     getValue("10.22.100.183:12000","ppfe-msmaster",14608);
//    printStream("10.24.33.123:12000");

//      for(int i =30;i<60;i++){
//        write("10.24.25.188:12000","tskey",i,"val"+i);
//      }


    //System.out.println(new String(cc.write("tskey".getBytes(),"val200".getBytes())));

      //System.out.println(new String(cc._write("tskey".getBytes(), Ints.toByteArray(2),"val0".getBytes()).get()));
    System.exit(0);
  }

  public static void getValue(String host, String colFam, int key){
    try{
      ChicagoClient cc = new ChicagoClient(host);
      cc.startAndWaitForNodes(1);
      System.out.println(new String(cc.read(colFam.getBytes(),Longs.toByteArray(key)).get().get(0)));
    }catch (Exception e){
      e.printStackTrace();
    }

  }

  public static void printStream(String host, String colFam){
    try {
      ChicagoClient cc = new ChicagoClient(host);
      cc.startAndWaitForNodes(1);
      System.out.println(host +" : "+new String(cc.stream(colFam.getBytes(), null).get().get(0)));
      cc.stop();
    }catch(Exception e){
      e.printStackTrace();
    }
  }

  public static void write(String host,String cf, int key, String value){
    try {
      ChicagoClient cc = new ChicagoClient(host);
      cc.startAndWaitForNodes(1);
      System.out.println(host +" : "+ new String(cc.tsWrite(cf.getBytes(),Longs.toByteArray(key),value.getBytes()).get().get(0)));
    }catch(Exception e){
      e.printStackTrace();
    }
  }

  public static void write(String host,String cf, String value){
    try {
      ChicagoClient cc = new ChicagoClient(host);
      cc.startAndWaitForNodes(1);
      System.out.println(host +" : "+Longs.fromByteArray(cc.tsWrite(cf.getBytes(),value.getBytes()).get().get(0)));
    }catch(Exception e){
      e.printStackTrace();
    }
  }

  public static void write1000(String host) throws Exception{
    String cf = "test";
    String key = "key                                                               ";
    ChicagoClient cc = new ChicagoClient(host);
    cc.startAndWaitForNodes(1);
    for (int i =200000;i<800000;i++){
      System.out.print(cc.write(cf.getBytes(),(key+i).getBytes(),("val                                                          " +
        "                                                                             " +
        "                                                                            "+i).getBytes()));
    }
  }


  public void readColumnFamily(String host) throws Exception
  {
    ChicagoClient cc = new ChicagoClient(host);
    cc.startAndWaitForNodes(1);
    List<String> lines = cc.scanColFamily();
    for(String line: lines) {
      System.out.println("ColFamily: " + line);
    }
  }


  public void readColumnFamilyKeys(String host, String colFam) throws Exception
  {
    ChicagoClient cc = new ChicagoClient(host);
    cc.startAndWaitForNodes(1);
    ListenableFuture<List<byte[]>> f = cc.scanKeys(colFam.getBytes());
    byte[] b = f.get().get(0);
    String resp = new String(b);
    String[] lines = resp.split("\0")[0].split("@@@");
    for(String line: lines) {
      System.out.println("Keys: " + line);
    }
  }
}
