package com.xjeffrose.chicago.examples;

import com.google.common.util.concurrent.ListenableFuture;
import com.xjeffrose.chicago.client.ChicagoStream;
import com.xjeffrose.chicago.client.ChicagoTSClient;

import java.util.Arrays;

/**
 * Created by root on 6/22/16.
 */
public class ChicagoStreamExample {
  ChicagoTSClient chicagoTSClient;

  public static void main(String[] args) throws Exception{
    ChicagoStreamExample cs = new ChicagoStreamExample();
    cs.chicagoTSClient = new ChicagoTSClient("10.24.25.188:2181,10.24.25.189:2181,10.25.145.56:2181,10.24.33.123:2181",3);
    cs.chicagoTSClient.startAndWaitForNodes(3);
    cs.transactStream();
  }


  public void transactStream() throws Exception {
    String key = "ppfe";
    byte[] offset = null;

    ListenableFuture<com.xjeffrose.chicago.client.ChicagoStream> f = chicagoTSClient.stream(key.getBytes());
    com.xjeffrose.chicago.client.ChicagoStream cs = f.get();
    ListenableFuture<byte[]> resp = cs.getStream();

    String result = new String(resp.get());
    cs.close();
    byte[] old=null;
    int count =0;
    while(result.contains("@@@")){

      offset = result.split("@@@")[1].getBytes();
      String[] lines = (result.split("@@@")[0]).split("\0");
      for(String line : lines){
        if(line.length()!= 0)
          System.out.println(line);
      }
      if(old != null && Arrays.equals(old,offset)){
        Thread.sleep(500);
      }

      ListenableFuture<com.xjeffrose.chicago.client.ChicagoStream> _f = chicagoTSClient.stream(key.getBytes(), offset);
      ChicagoStream newcs = _f.get();
      ListenableFuture<byte[]> newresp = newcs.getStream();
      result = new String(newresp.get());
      old = offset;
    }

    ListenableFuture<com.xjeffrose.chicago.client.ChicagoStream> _f = chicagoTSClient.stream(key.getBytes(), offset);
    cs = _f.get();
    ListenableFuture<byte[]> _resp = cs.getStream();

    System.out.println(new String(_resp.get()));
  }
}
