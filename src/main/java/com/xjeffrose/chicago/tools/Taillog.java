package com.xjeffrose.chicago.tools;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.primitives.Longs;
import com.xjeffrose.chicago.ChiUtil;
import com.xjeffrose.chicago.client.ChicagoClient;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  @Parameter(names = {"--startTime","-s"}, description = "Start time for Logs in format yyyy-MM-dd'T'HH:mm:ss")
  private String startTime;

  @Parameter(names = {"--endTime","-e"}, description = "end time for Logs yyyy-MM-dd'T'HH:mm:ss")
  private String endTime;

  @Parameter(names = {"--infinteLoop","-i"}, description = "Keep streaming infintely ?")
  private Boolean infinte = false;

  @Parameter(names = {"--help","-h"}, description="Show usage", help = true)
  private boolean help;
  private static final Logger log = LoggerFactory.getLogger(Taillog.class.getName());
  private ChicagoClient chicagoClient;
  private Date lastTime;

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

    chicagoClient = new ChicagoClient(zkString,3);
    chicagoClient.startAndWaitForNodes(3);
    if(startTime != null){
      long startOffset = getNearestOffset(topic, startTime);
      printStream(topic,startOffset,endTime);
    }else {
      printStream(topic, offset, null);
    }
    System.exit(0);
  }

  public void printStream(String key, long offset, String endTime) throws Exception{
    Date endDateTime = null;
    if(endTime != null) {
      DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
      endDateTime = dateFormat.parse(endTime);
    }
    byte[] resp = null;
    if(offset == -1){
      resp = chicagoClient.stream(key.getBytes()).get().get(0);
    }else {
      resp = chicagoClient.stream(key.getBytes(), Longs.toByteArray(offset)).get().get(0);
    }

    byte[] resultArray = resp;
    String result = new String(resultArray);
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
          if(endDateTime !=null && endDateTime.after(getDate(line))){
            System.out.println("End time reached.");
            System.exit(0);
          }
          printLine(line);
          count++;
        }
      }
      if(count > 0){
        offset = offset + 1;
      }
      if(old != -1 && (old == offset)){
        if(!infinte){
          System.out.println("Reached the end of stream ");
          System.exit(0);
        }
        Thread.sleep(500);
      }

      //while(true) {
      //  try {
      //    ListenableFuture<com.xjeffrose.chicago.client.ChicagoStream> _f =
      //      chicagoTSClient.stream(key.getBytes(), Longs.toByteArray(offset));
      //    newcs = _f.get();
      //    break;
      //  } catch (Exception e) {
      //    e.printStackTrace();
      //    Thread.sleep(100);
      //  }
      //}
      resultArray = chicagoClient.stream(key.getBytes(), Longs.toByteArray(offset)).get().get(0);
      result = new String(resultArray);
      old = offset;
    }
    return;
  }

  public long getNearestOffset(String key, String startTime) throws Exception{
    long offset = -1;
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    Date targetDate = dateFormat.parse(startTime);

    long startOffset = 0;
    long endOffset = getLastOffset(key);
    if(endOffset == -1){
      log.error("Cannot find last offset");
    }

    if(debug) {
      log.info("End Offset = " + endOffset);
    }
    Date startDate = getDate(key,startOffset);
    if(startDate == null){
      System.out.println("Cannot find earliest date");
      System.exit(0);
    }
    if(debug) {
      log.info("Start date = " + startDate.toString());
    }

    if(targetDate.before(startDate)){
      System.out.println("start time is earlier than chicago logs");
      System.exit(0);
    }

    Date endDate = this.lastTime;
    if(endDate == null){
      log.error("Cannot find last date");
      System.exit(0);
    }
    if(debug) {
      log.info("End date = " + endDate.toString());
    }

    if(targetDate.after(endDate)){
      System.out.println("start time is ahead of chicago logs");
      System.exit(0);
    }
    System.out.print("Finding offset.");
    while(endOffset > startOffset){
      long midOffset = (endOffset + startOffset)/2;

      Date midDate = getDate(key,midOffset);
      if(midDate.before(targetDate) && (TimeUnit.MILLISECONDS.toMinutes(targetDate.getTime()-midDate.getTime()) < 10)){
        return midOffset;
      }

      if(midDate.after(targetDate)){
        endOffset = midOffset;
      }else{
        startOffset = midOffset;
      }
      System.out.print(" .");
    }
    return offset;
  }

  public Long getLastOffset(String key) {
    long endOffset=-1;
    try {
      byte[] resultArray = chicagoClient.stream(key.getBytes()).get().get(0);
      String result = new String(resultArray);
      if (result.contains(ChiUtil.delimiter)) {
        endOffset = ChiUtil.findOffset(resultArray);
        this.lastTime = getDate(result);
      }
    }catch (Exception e){
      e.printStackTrace();
    }
    return endOffset;
  }


  public Date getDate(String key, Long offset){
    String data = null;
    try {
      data =
        new String(chicagoClient.read(key.getBytes(), Longs.toByteArray(offset)).get().get(0));
    } catch (Exception e){
      e.printStackTrace();
    }
    Date date = null;
    if(data != null && !data.isEmpty()) {
      date = getDate(data);
    }
    return date;
  }

  public Date getDate(String data){
    try {
      return new Date(Long.parseLong(data.split(" ")[3].split("\\.")[1].substring(3)));
    }catch (Exception e){
      e.printStackTrace();
    }
    return null;
  }

  public void printLine(String line){
    String responseCode ="";
    try{
       responseCode =line.split("ResponseCode:")[1].split(" ")[2];
    } catch (Exception e){

    }
    String cc;
    try {
      if (line.startsWith("E")) {
        cc = ColorCodes.ANSI_RED;
      } else if (line.startsWith("W")) {
        cc = ColorCodes.ANSI_WHITE;
      } else {
        cc = ColorCodes.ANSI_BLUE;
        if (responseCode.equals("200")) {
          cc = ColorCodes.ANSI_GREEN;
        } else if (responseCode.startsWith("4")) {
          cc = ColorCodes.ANSI_RED;
        } else if (responseCode.startsWith("5")) {
          cc = ColorCodes.ANSI_WHITE;
        }
      }
      System.out.println(cc + line + ColorCodes.ANSI_RESET);
    }finally {
      System.out.print(ColorCodes.ANSI_RESET);
    }
  }


}
