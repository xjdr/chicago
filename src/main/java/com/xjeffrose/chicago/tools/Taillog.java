package com.xjeffrose.chicago.tools;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ListenableFuture;
import com.xjeffrose.chicago.ChiUtil;
import com.xjeffrose.chicago.Chicago;
import com.xjeffrose.chicago.client.ChicagoStream;
import com.xjeffrose.chicago.client.ChicagoTSClient;

/**
 * Created by root on 7/14/16.
 */
public class Taillog {
  @Parameter(names={"--topic", "-t"}, description = "Topic name to query the logs from")
  String topic="ppfe-msmaster";

  @Parameter(names={"--offset", "-o"}, description = "Offset from which the logs need to be fetched."
    + " 0, for beginning of time; -1, to get latest logs")
  long offset = -1;

  @Parameter(names={"--zkstring", "-z"}, description = "zookeeper connection string for chicago servers")
  String zkString="10.22.100.183:2181,10.25.180.234:2181,10.22.103.86:2181,10.25.180.247:2181,10.25.69.226:2181";

  @Parameter(names = {"--debug","-d"}, description = "Debug mode")
  private boolean debug = false;

  @Parameter(names = "--help", description="Show usage", help = true)
  private boolean help;


  private ChicagoTSClient chicagoTSClient;

  public static void main(String ... args) throws Exception{
    Taillog main = new Taillog();
    JCommander j = new JCommander(main, args);
    main.run(j);
  }

  public void run(JCommander jCommander) throws Exception{

    if(help){
      jCommander.usage();
      return;
    }

    chicagoTSClient = new ChicagoTSClient(zkString,3);
    chicagoTSClient.startAndWaitForNodes(3);
    printStream(topic,offset);

  }

  public void printStream(String key, long offset) throws Exception{
    ListenableFuture<ChicagoStream> f;
    if(offset == -1){
      f = chicagoTSClient.stream(key.getBytes());
    }else {
      f = chicagoTSClient.stream(key.getBytes(), Longs.toByteArray(offset));
    }
    com.xjeffrose.chicago.client.ChicagoStream cs = f.get();
    ListenableFuture<byte[]> resp = cs.getStream();

    byte[] resultArray = resp.get();
    String result = new String(resultArray);
    cs.close();
    long old=-1;
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
          if(debug) {
            System.out.print("Last offset =" + offset + ":");
          }
          printLine(line);
          count++;
        }
      }
      if(count > 0){
        offset = offset + 1;
      }
      if(old != -1 && (old == offset)){
        Thread.sleep(500);
      }

      ListenableFuture<com.xjeffrose.chicago.client.ChicagoStream> _f = chicagoTSClient.stream(key.getBytes(), Longs.toByteArray(offset));
      ChicagoStream newcs = _f.get();
      ListenableFuture<byte[]> newresp = newcs.getStream();
      resultArray = newresp.get();
      result = new String(resultArray);
      old = offset;
      newcs.close();
    }
  }

  public void printLine(String line){
    String responseCode ="";
    try{
       responseCode =line.split("ResponseCode:")[1].split(" ")[2];
    } catch (Exception e){

    }
    String cc;

    if(line.startsWith("E")){
      cc = ColorCodes.ANSI_RED;
    }else if(line.startsWith("W")){
      cc= ColorCodes.ANSI_WHITE;
    }else{
      cc= ColorCodes.ANSI_BLUE;
      if(responseCode.equals("200")){
        cc = ColorCodes.ANSI_GREEN;
      }else if(responseCode.startsWith("4")){
        cc = ColorCodes.ANSI_RED;
      }else if(responseCode.startsWith("5")){
        cc = ColorCodes.ANSI_WHITE;
      }
    }

    System.out.println(cc + line + ColorCodes.ANSI_RESET);
  }


}
