package com.xjeffrose.chicago.client.tools;

import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.xjeffrose.chicago.client.ChicagoClient;
import com.xjeffrose.chicago.client.ChicagoClientException;
import com.xjeffrose.chicago.client.ChicagoClientTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

/**
 * Created by root on 6/23/16.
 */
public class WritePerformance implements Runnable {
  private final static String key = "ppfe-test";
  ChicagoClient cts;
  private final CountDownLatch latch;
  private static AtomicInteger success = new AtomicInteger(0);
  private static AtomicInteger failure = new AtomicInteger(0);
  private static AtomicInteger readSuccess = new AtomicInteger(0);
  private static AtomicInteger readFailure = new AtomicInteger(0);
  private static AtomicInteger timeouts = new AtomicInteger(0);
  private final static List<ListenableFuture<byte[]>> futures = new ArrayList<ListenableFuture<byte[]>>();
  private static Long[] keys;
  int valCount;

  public WritePerformance(CountDownLatch latch,ChicagoClient cts, int valCount){
    this.latch = latch;
    this.cts=cts;
    this.valCount=valCount;
  }

  @Override
  public void run(){
    try{
      String v = "val" +valCount + "TTE";
      byte[] val = v.getBytes();
      System.arraycopy(v.getBytes(),0,val,0,v.getBytes().length);
      ListenableFuture<List<byte[]>> future = cts.tsWrite(key.getBytes(),val);
      Futures.addCallback(future, new FutureCallback<List<byte[]>>() {
        @Override
        public void onSuccess(@Nullable List<byte[]> bytes) {
          if(!bytes.isEmpty()) {
            long o = Longs.fromByteArray(bytes.get(0));
            //System.out.println(o);
          }else{
            System.out.println("Failed "+ v);
          }
          success.getAndIncrement();
          latch.countDown();
        }

        @Override
        public void onFailure(Throwable throwable) {
          // TODO(JR): Maybe Try again?
          //throwable.printStackTrace();
          failure.getAndIncrement();
          latch.countDown();
        }
      });
    } catch (ChicagoClientTimeoutException e){
      System.out.println(e.getMessage());
      latch.countDown();
      failure.getAndIncrement();
      e.printStackTrace();
    } catch (ChicagoClientException e) {
      System.out.println(e.getMessage());
      failure.getAndIncrement();
      latch.countDown();
      e.printStackTrace();
    } finally {

    }
  }

  public static void main(String[] args) throws Exception {

    final int loop = Integer.parseInt(args[0]);
    final int workerSize = Integer.parseInt(args[1]);
    final int clients = Integer.parseInt(args[2]);
    final String connectionString = args[3];
    ExecutorService executor = Executors.newFixedThreadPool(workerSize);
    CountDownLatch latch = new CountDownLatch(loop);
    ChicagoClient[] ctsa = new ChicagoClient[clients];
    keys = new Long[loop];
    for(int i =0;i<clients;i++){
      if(connectionString.contains("2181")){
        //Jeff servers = 10.22.100.183:2181,10.25.180.234:2181,10.22.103.86:2181,10.25.180.247:2181,10.25.69.226:2181
        //smadan server = 10.24.25.188:2181,10.24.25.189:2181,10.25.145.56:2181,10.24.33.123:2181
        ctsa[i] = new ChicagoClient(connectionString,3);
        ctsa[i].startAndWaitForNodes(3);
      }else {
        ctsa[i] = new ChicagoClient(connectionString);
      }
    }
    Thread.sleep(500);
    Random r = new Random();
    System.out.println("########       Statring writes        #########");
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < loop; i++) {
      executor.submit(new WritePerformance(latch, ctsa[i % clients], i));
      if(i%5 ==0) {
        //long start = System.nanoTime();
        //long delay = 10000;
        //while(System.nanoTime() - start < delay);
        Thread.sleep(0,5);
      }
    }

    latch.await();

    System.out.println("Total time taken for "+loop+ " writes ="+ (System.currentTimeMillis() - startTime) + "ms");
    System.out.println("Total success :"+ success.get() + " Failures :"+ failure.get() + "Timeouts :"+ timeouts.get());
    System.out.println("########       Writes completed       #########");
    System.out.println();
    System.out.println();

    //System.out.println("########       Verifying the writes        #########");
    //System.out.println("Randomly reading 5 values");
    //Random ran = new Random();
    //for(int i =0;i<5;i++){
    //
    //  long curkey = keys[ran.nextInt(loop)];
    //    try{
    //      String returnVal = new String(ctsa[(int)curkey%clients].read(key.getBytes(), Longs.toByteArray(curkey)).get());
    //      //System.out.println(curkey +" :"+returnVal);
    //      if(returnVal.startsWith("val")){
    //        readSuccess.getAndIncrement();
    //      }else{
    //        readFailure.getAndIncrement();
    //      }
    //    }catch(Exception e){
    //      e.printStackTrace();
    //      readFailure.getAndIncrement();
    //    }finally {
    //    }
    //}
    //System.out.println("Total read success :"+ readSuccess.get() + " Failures :"+ readFailure.get());
    executor.shutdownNow();

    System.exit(0);
  }
}
