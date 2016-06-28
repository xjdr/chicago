package com.xjeffrose.chicago.examples;

import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import com.xjeffrose.chicago.ChiUtil;
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
    System.exit(0);
  }


  public void transactStream() throws Exception {
    String key = "writeTestKey";
    int offset = -1;

    ListenableFuture<com.xjeffrose.chicago.client.ChicagoStream> f = chicagoTSClient.stream(key.getBytes());
    com.xjeffrose.chicago.client.ChicagoStream cs = f.get();
    ListenableFuture<byte[]> resp = cs.getStream();

    byte[] resultArray = resp.get();
    String result = new String(resultArray);
    cs.close();
    int old=-1;
    while(true){
      if(!result.contains(ChiUtil.delimiter)){
        System.out.println("No delimetr present");
        System.out.println(result);
        break;

      }

      offset = ChiUtil.findOffset(resultArray);
      String[] lines = (result.split(ChiUtil.delimiter)[0]).split("\0");
      int count =0;
      for(String line : lines){
        if(line.length()!= 0) {
          System.out.println(offset +":" +line);
          count++;
        }
      }
      if(count > 0){
        offset = offset + 1;
      }
      if(old != -1 && (old == offset)){
        Thread.sleep(500);
      }

      ListenableFuture<com.xjeffrose.chicago.client.ChicagoStream> _f = chicagoTSClient.stream(key.getBytes(), Ints.toByteArray(offset));
      ChicagoStream newcs = _f.get();
      ListenableFuture<byte[]> newresp = newcs.getStream();
      resultArray = newresp.get();
      result = new String(resultArray);
      old = offset;
      newcs.close();
    }

  }
}
