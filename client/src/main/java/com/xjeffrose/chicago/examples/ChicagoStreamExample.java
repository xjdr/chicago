package com.xjeffrose.chicago.examples;

import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ListenableFuture;
import com.xjeffrose.chicago.ChiUtil;
import com.xjeffrose.chicago.client.ChicagoAsyncClient;
import com.xjeffrose.chicago.client.ChicagoClientException;
import com.xjeffrose.chicago.client.ChicagoClientTimeoutException;
import com.xjeffrose.chicago.client.ChicagoClient;

import io.netty.buffer.ByteBuf;
import java.util.List;

/**
 * Created by root on 6/22/16.
 */
public class ChicagoStreamExample {
  ChicagoAsyncClient chicagoClient;

  //private static String key = "ppfe-msmaster-LM-SJN-00875858";
  private static String key = "ppfe-tests";

  public static void main(String[] args) throws Exception{
    ChicagoStreamExample cs = new ChicagoStreamExample();
    cs.chicagoClient = new ChicagoAsyncClient("10.24.25.188:2181,10.24.25.189:2181,10.25.145.56:2181,10.24.33.123:2181",3);
    cs.chicagoClient.start();

    //cs.chicagoClient.startAndWaitForNodes(4);
    //cs.writeSomeData();
    //cs.transactStream();
    //cs.transactStreamWithBuf();
    cs.printStream();
    System.exit(0);
  }

  public void printStream() throws  Exception{
    Long offset = 3L;
    byte[] resultArray = chicagoClient.stream(key.getBytes(), Longs.toByteArray(offset)).get();
    String result = new String(resultArray);
    System.out.println(result);
  }

  public void writeSomeData() throws Exception{
    String val = "Test data";
    long startTime = System.currentTimeMillis();
    for(int i =0;i<100000;i++){
      try {
        System.out.println(i);
        System.out.println("Response = " + Longs.fromByteArray(chicagoClient.tsWrite(key.getBytes(), (i+val).getBytes()).get()));
        //Thread.sleep(500);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    System.out.println("Total time taken :"+ (System.currentTimeMillis() - startTime) + "ms");
  }


//  public void transactStream() throws Exception {
//    Long offset = 0L;
//
//    ListenableFuture<List<byte[]>> resp = chicagoClient.stream(key.getBytes(), Longs.toByteArray(offset));
//
//    byte[] resultArray = resp.get().get(0);
//    String result = new String(resultArray);
//    long old=-1;
//    while(true){
//      if(!result.contains(ChiUtil.delimiter)){
//        System.out.println("No delimetr present");
//        System.out.println(result);
//        break;
//
//      }
//
//      offset = ChiUtil.findOffset(resultArray);
//      String[] lines = (result.split(ChiUtil.delimiter)[0]).split("\0");
//      int count =0;
//      for(String line : lines){
//        if(line.length()!= 0) {
//          System.out.println(offset +":" +line);
//          count++;
//        }
//      }
//      if(count > 0){
//        offset = offset + 1;
//      }
//      if(old != -1 && (old == offset)){
//        Thread.sleep(500);
//      }
//
//      ListenableFuture<List<byte[]>> newresp = chicagoClient.stream(key.getBytes(), Longs.toByteArray(offset));
//      resultArray = newresp.get().get(0);
//      result = new String(resultArray);
//      old = offset;
//    }
//  }
//
//  public void transactStreamWithBuf() throws Exception {
//    ByteBuf buffer = chicagoClient.aggregatedStream(key.getBytes(),Longs.toByteArray(9999));
//    int readableBytes = buffer.readableBytes();
//    int i =0;
//    while(true) {
//      if (readableBytes > 0) {
//        byte[] data = new byte[buffer.readableBytes()];
//        try {
//          buffer.readBytes(data);
//          String stringData = new String(data);
//          String[] lines = (stringData).split("\0");
//          for (String line : lines) {
//            line.replace('\n',' ');
//            System.out.println(++i + line);
//            if(i>400000){
//              break;
//            }
//          }
//        }catch (Exception e){
//          e.printStackTrace();
//        }
//      } else {
//        Thread.sleep(100);
//      }
//      readableBytes = buffer.readableBytes();
//    }
//  }
}
