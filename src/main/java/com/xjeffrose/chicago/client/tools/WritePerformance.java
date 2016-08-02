package com.xjeffrose.chicago.client.tools;

import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.xjeffrose.chicago.client.ChicagoClient;
import com.xjeffrose.chicago.client.ChicagoClientException;
import com.xjeffrose.chicago.client.ChicagoClientTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

/**
 * Created by root on 6/23/16.
 */
public class WritePerformance{
  private final static String key = "ppfe-test-cc";
  ChicagoClient cts;
  //private final CountDownLatch latch;
  private static AtomicInteger success = new AtomicInteger(0);
  private static AtomicInteger failure = new AtomicInteger(0);
  private static AtomicInteger readSuccess = new AtomicInteger(0);
  private static AtomicInteger readFailure = new AtomicInteger(0);
  private static AtomicInteger timeouts = new AtomicInteger(0);
  private static final long NS_PER_MS = 1000000L;
  private static final long NS_PER_SEC = 1000 * NS_PER_MS;
  private static final long MIN_SLEEP_NS = 2 * NS_PER_MS;
  private final static List<ListenableFuture<byte[]>> futures = new ArrayList<ListenableFuture<byte[]>>();
  private static Long[] keys;
  int valCount;

  public static void main(String[] args) {

    final int loop = Integer.parseInt(args[0]);
    final int workerSize = Integer.parseInt(args[1]);
    final int clients = Integer.parseInt(args[2]);
    int throughput = Integer.parseInt(args[3]);
    final String connectionString = args[4];
    CountDownLatch latch = new CountDownLatch(loop);
    ChicagoClient[] ctsa = new ChicagoClient[clients];
    keys = new Long[loop];
    for(int i =0;i<clients;i++){
      if(connectionString.contains("2181")){
        //Jeff servers = 10.22.100.183:2181,10.25.180.234:2181,10.22.103.86:2181,10.25.180.247:2181,10.25.69.226:2181
        //smadan server = 10.24.25.188:2181,10.24.25.189:2181,10.25.145.56:2181,10.24.33.123:2181
        try {
          ctsa[i] = new ChicagoClient(connectionString,3);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        //ctsa[i].startAndWaitForNodes(3);
      }else {
        try {
          ctsa[i] = new ChicagoClient(connectionString);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
    //Thread.sleep(500);
    long sleepTime = NS_PER_SEC / throughput;
    long sleepDeficitNs = 0;
    Stats stats = new Stats(loop,5000, latch);
    System.out.println("########       Statring writes        #########");
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < loop; i++) {
      long sendStart = System.currentTimeMillis();
      String v = "val" +i + "TTE-cc";
      Callback cb = stats.nextCompletion(sendStart, v.getBytes().length, stats);
      ListenableFuture<List<byte[]>> future = null;
      try {
        future = ctsa[i%clients].tsWrite(key.getBytes(),v.getBytes());
        Futures.addCallback(future,cb);
      } catch (ChicagoClientTimeoutException e) {
        e.printStackTrace();
      } catch (ChicagoClientException e) {
        e.printStackTrace();
      }
      if (throughput > 0) {
        sleepDeficitNs += sleepTime;
        if (sleepDeficitNs >= MIN_SLEEP_NS) {
          long sleepMs = sleepDeficitNs / 1000000;
          long sleepNs = sleepDeficitNs - sleepMs * 1000000;
          try {
            Thread.sleep(sleepMs, (int) sleepNs);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          sleepDeficitNs = 0;
        }
      }
    }

    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    stats.printTotal();
    Long totalTime = (System.currentTimeMillis() - startTime);
    System.out.println("Total time taken for "+loop+ " writes ="+ totalTime + "ms" +  " average ="+ (totalTime/(float)loop)+"ms");
    System.out.println("Total success :"+ success.get() + " Failures :"+ failure.get() + "Timeouts :"+ timeouts.get());
    System.out.println("########       Writes completed       #########");
    System.out.println();
    System.out.println();
    System.exit(0);
  }

  private static class Stats {
    private long start;
    private long windowStart;
    private int[] latencies;
    private int sampling;
    private int iteration;
    private int index;
    private long count;
    private long bytes;
    private int maxLatency;
    private long totalLatency;
    private long windowCount;
    private int windowMaxLatency;
    private long windowTotalLatency;
    private long windowBytes;
    private long reportingInterval;
    private final CountDownLatch latch;

    public Stats(long numRecords, int reportingInterval, CountDownLatch latch) {
      this.start = System.currentTimeMillis();
      this.windowStart = System.currentTimeMillis();
      this.index = 0;
      this.iteration = 0;
      this.sampling = (int) (numRecords / Math.min(numRecords, 500000));
      this.latencies = new int[(int) (numRecords / this.sampling) + 1];
      this.index = 0;
      this.maxLatency = 0;
      this.totalLatency = 0;
      this.windowCount = 0;
      this.windowMaxLatency = 0;
      this.windowTotalLatency = 0;
      this.windowBytes = 0;
      this.totalLatency = 0;
      this.reportingInterval = reportingInterval;
      this.latch = latch;
    }

    public void record(int iter, int latency, int bytes, long time) {
      this.count++;
      this.bytes += bytes;
      this.totalLatency += latency;
      this.maxLatency = Math.max(this.maxLatency, latency);
      this.windowCount++;
      this.windowBytes += bytes;
      this.windowTotalLatency += latency;
      this.windowMaxLatency = Math.max(windowMaxLatency, latency);
      if (iter % this.sampling == 0) {
        this.latencies[index] = latency;
        this.index++;
      }
            /* maybe report the recent perf */
      if (time - windowStart >= reportingInterval && Thread.currentThread().getName().contains("10")) {
        printWindow();
        newWindow();
      }
    }

    public Callback nextCompletion(long start, int bytes, Stats stats) {
      Callback cb = new Callback(this.iteration, start, bytes, stats, latch);
      this.iteration++;
      return cb;
    }

    public synchronized void printWindow() {
      long ellapsed = System.currentTimeMillis() - windowStart;
      double recsPerSec = 1000.0 * windowCount / (double) ellapsed;
      double mbPerSec = 1000.0 * this.windowBytes / (double) ellapsed / (1024.0 * 1024.0);
      System.out.printf("%d records sent, %.1f records/sec (%.2f MB/sec), %.1f ms avg latency, %.1f max latency.\n",
        windowCount,
        recsPerSec,
        mbPerSec,
        windowTotalLatency / (double) windowCount,
        (double) windowMaxLatency);
    }

    public void newWindow() {
      this.windowStart = System.currentTimeMillis();
      this.windowCount = 0;
      this.windowMaxLatency = 0;
      this.windowTotalLatency = 0;
      this.windowBytes = 0;
    }

    public void printTotal() {
      long ellapsed = System.currentTimeMillis() - start;
      double recsPerSec = 1000.0 * count / (double) ellapsed;
      double mbPerSec = 1000.0 * this.bytes / (double) ellapsed / (1024.0 * 1024.0);
      int[] percs = percentiles(this.latencies, index, 0.5, 0.95, 0.99, 0.999);
      System.out.printf("%d records sent, %f records/sec (%.2f MB/sec), %.2f ms avg latency, %.2f ms max latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.\n",
        count,
        recsPerSec,
        mbPerSec,
        totalLatency / (double) count,
        (double) maxLatency,
        percs[0],
        percs[1],
        percs[2],
        percs[3]);
    }

    private static int[] percentiles(int[] latencies, int count, double... percentiles) {
      int size = Math.min(count, latencies.length);
      Arrays.sort(latencies, 0, size);
      int[] values = new int[percentiles.length];
      for (int i = 0; i < percentiles.length; i++) {
        int index = (int) (percentiles[i] * size);
        values[i] = latencies[index];
      }
      return values;
    }
  }

  private static class Callback implements FutureCallback<List<byte[]>>{
    private final long start;
    private final Stats stats;
    private final int iteration;
    private final int nbytes;
    private final CountDownLatch latch;

    public Callback(int iter, long start,int bytes, Stats stats, CountDownLatch latch) {
      this.start = start;
      this.stats = stats;
      this.iteration = iter;
      this.nbytes = bytes;
      this.latch = latch;
    }

    @Override public void onSuccess(@Nullable List<byte[]> bytes) {
      long now = System.currentTimeMillis();
      int latency = (int) (now - start);
      this.stats.record(iteration, latency, nbytes, now);
      latch.countDown();
    }

    @Override public void onFailure(Throwable throwable) {
      System.out.println("Failed...");
      latch.countDown();
    }
  }

}
