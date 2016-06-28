package com.xjeffrose.chicago.examples;

import com.google.common.primitives.Ints;
import com.xjeffrose.chicago.client.*;

import java.util.concurrent.ExecutionException;

/**
 * Created by root on 6/10/16.
 */
public class SingleServerClient {

  public static void main(String[] args) throws InterruptedException, ChicagoClientTimeoutException, ExecutionException, ChicagoClientException {
    //ChicagoClient cc = new ChicagoClient("10.24.25.188:2181,10.24.25.189:2181,10.24.33.123:2181,10.25.145.56:2181",3);
    //cc.startAndWaitForNodes(3);
//    cc.deleteColFam("tsRepKey".getBytes()).get();

  //write("10.24.25.188:12000","tskey",34,"val34");
    //printStream("10.24.25.188:12000", "testKey");
    getValue("10.24.33.123:12000", "writeTestKey",496);
    getValue("10.24.25.189:12000", "writeTestKey",496);
    getValue("10.25.145.56:12000", "writeTestKey",496);
    //printStream("10.24.25.189:12000");
     //printStream("10.25.145.56:12000");
     //getValue("10.25.145.56:12000","tsRepKey",90);
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
      System.out.println(new String(cc.read(colFam.getBytes(),Ints.toByteArray(key)).get()));
    }catch (Exception e){
      e.printStackTrace();
    }

  }

  public static void printStream(String host, String colFam){
    try {
      ChicagoTSClient cc = new ChicagoTSClient(host);
      System.out.println(host +" : "+new String(cc.stream(colFam.getBytes(), null).get().getStream().get()));
      cc.stop();
    }catch(Exception e){
      e.printStackTrace();
    }
  }

  public static void write(String host,String cf, int key, String value){
    try {
      ChicagoTSClient cc = new ChicagoTSClient(host);
      System.out.println(host +" : "+ new String(cc._write(cf.getBytes(),Ints.toByteArray(key),value.getBytes()).get()));
    }catch(Exception e){
      e.printStackTrace();
    }
  }

  public static void write(String host,String cf, String value){
    try {
      ChicagoTSClient cc = new ChicagoTSClient(host);
      System.out.println(host +" : "+Ints.fromByteArray(cc.write(cf.getBytes(),value.getBytes())));
    }catch(Exception e){
      e.printStackTrace();
    }
  }
}
