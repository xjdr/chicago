package com.xjeffrose.chicago.appender;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;
import com.xjeffrose.chicago.client.ChicagoTSClient;

import java.nio.ByteBuffer;
import java.util.Date;

public class ChicagoAppender extends AppenderSkeleton{
    private String chicagoZk;
    private String key;
    private int pool;
    private static ExecutorService executor = Executors.newFixedThreadPool(10);
    private final ConcurrentLinkedDeque<ChicagoTSClient> queue = new ConcurrentLinkedDeque<>();

    private class Write implements Runnable {
        private ChicagoTSClient cts;
        private String message;
        private ChicagoAppender appender;
        public Write(ChicagoTSClient cts, String message, ChicagoAppender appender) {
            this.cts = cts;
            this.message = message;
            this.appender = appender;
        }

        @Override
        public void run() {
            try {
                cts.write(key.getBytes(), message.getBytes());
            } catch (Exception e) {
                e.printStackTrace();
                appender.getErrorHandler().error(message);
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
    public void activateOptions(){
        if(chicagoZk == null) {
            throw new RuntimeException("Chicago Log4j Appender: chicago ZK not configured!");
        }

        if(key == null){
            throw new RuntimeException("Chicago Log4j Appender: chicago key not configured!");
        }
        if(pool == 0){
            pool = 4;
        }

        try {
            for(int i =0; i< pool;i++) {
                LogLog.debug("Attempting connection");
                ChicagoTSClient cs=  new ChicagoTSClient(chicagoZk, 3);
                cs.startAndWaitForNodes(3,2000);
                queue.add(cs);
                LogLog.debug("Chicago connected to " + chicagoZk);
            }
        }catch (InterruptedException exception) {
            throw new RuntimeException(exception);
        }
    }
    @Override
    protected void append(LoggingEvent loggingEvent) {

        Long timeStamp = loggingEvent.getTimeStamp();
        String message = subAppend(loggingEvent);
        LogLog.debug("[" + new Date(timeStamp) + "]" + message);
        executor.submit(new Write(getNext(),message,this));
    }

    private ChicagoTSClient getNext(){
        ChicagoTSClient cts = queue.removeFirst();
        queue.addLast(cts);
        return cts;
    }

    private String subAppend(LoggingEvent event) {
        return (this.layout == null) ? event.getRenderedMessage() : this.layout.format(event);
    }

    private byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    public void close() {
        executor.shutdownNow();
        try {
            while(queue.peekFirst() != null){
                ChicagoTSClient cts = queue.removeFirst();
                cts.stop();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        if(!this.closed) {
            this.closed = true;
        }

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
