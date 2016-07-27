package com.xjeffrose.chicago.appender;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.xjeffrose.chicago.Chicago;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.RollingFileAppender;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;
import com.xjeffrose.chicago.client.ChicagoClient;

public class ChicagoAppender extends AppenderSkeleton{
    private String chicagoZk;
    private String key;
    private int pool;
    private String fileName;
    private ChicagoAppender self;
    private final static ConcurrentLinkedQueue<String> buffer = new ConcurrentLinkedQueue<>();
    private final static ExecutorService schedulerExecutor = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder()
        .setNameFormat("chicago-appender-scheduler")
        .build());

    private final static ExecutorService executor = Executors.newFixedThreadPool(5,
    new ThreadFactoryBuilder()
      .setNameFormat("chicago-appender-worker-%d")
      .build());
    private final ConcurrentLinkedDeque<ChicagoClient> clientQueue = new ConcurrentLinkedDeque<>();
    private final RollingFileAppender fileAppender = new RollingFileAppender();

    private class Write implements Runnable {
        private ChicagoClient cts;
        private String message;
        private ChicagoAppender appender;
        public Write(ChicagoClient cts, String message, ChicagoAppender appender) {
            this.cts = cts;
            this.message = message;
            this.appender = appender;
        }

        @Override
        public void run() {
            try {
                if(message.contains("--------")){
                  System.out.println(message);
                }
                cts.tsWrite(key.getBytes(), message.getBytes());
            } catch (Exception e) {
                System.out.println(message);
                e.printStackTrace();
            }
        }
    }

    public String getChicagoZk() {
        return chicagoZk;
    }

    public void setChicagoZk(String chicagoZk) {
        this.chicagoZk = chicagoZk;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    @Override
    public void activateOptions() {
      if (chicagoZk == null) {
        throw new RuntimeException("Chicago Log4j Appender: chicago ZK not configured!");
      }

      if (key == null) {
        throw new RuntimeException("Chicago Log4j Appender: chicago key not configured!");
      }
      if (pool == 0) {
        pool = 4;
      }

      try {
        for (int i = 0; i < pool; i++) {
          LogLog.debug("Attempting connection");
          ChicagoClient cs = new ChicagoClient(chicagoZk, 3);
          cs.startAndWaitForNodes(3, 2000);
          clientQueue.add(cs);
          LogLog.debug("Chicago connected to " + chicagoZk);
          self = this;
        }
      } catch (InterruptedException exception) {
        LogLog.debug("Chicago Appender failed to initialize!!!");
      }

      schedulerExecutor.submit(new Runnable() {
        @Override
        public void run() {
          pushToChicago(self);
        }
      });
    }

    public void pushToChicago(ChicagoAppender appender){
      while(true) {
        if (!buffer.isEmpty()) {
          executor.submit(new Write(getNext(), buffer.poll(), appender));
        } else {
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            e.printStackTrace();
            break;
          }
        }
      }
    }

    @Override
    protected void append(LoggingEvent loggingEvent) {
        Long timeStamp = loggingEvent.getTimeStamp();
        String message = subAppend(loggingEvent);
        buffer.add(message);
    }

    private ChicagoClient getNext(){
        ChicagoClient cts = clientQueue.removeFirst();
        clientQueue.addLast(cts);
        return cts;
    }

    private String subAppend(LoggingEvent event) {
        return (this.layout == null) ? event.getRenderedMessage() : this.layout.format(event);
    }

    public void close() {
        executor.shutdownNow();
        schedulerExecutor.shutdownNow();
        try {
            while(clientQueue.peekFirst() != null){
                ChicagoClient cts = clientQueue.removeFirst();
                cts.stop();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        if(!this.closed) {
            this.closed = true;
        }
    }

    public void setFileName(String fileName){
      this.fileName = fileName;
    }

    public boolean requiresLayout() {
        return true;
    }

    public int getPool() {
        return pool;
    }

    public void setPool(int pool) {
        this.pool = pool;
    }
}
