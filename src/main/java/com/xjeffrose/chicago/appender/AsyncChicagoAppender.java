package com.xjeffrose.chicago.appender;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.xjeffrose.chicago.client.ChicagoClient;
import com.xjeffrose.chicago.client.ChicagoClientException;
import com.xjeffrose.chicago.client.ChicagoClientTimeoutException;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;

public class AsyncChicagoAppender extends AppenderSkeleton {

  private String chicagoZk;
  private String key;
  private ChicagoClient cs;

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

    try {
      cs = new ChicagoClient(chicagoZk, 3);
    } catch (InterruptedException e) {
    }

    LogLog.debug("Chicago connected to " + chicagoZk);
  }

  @Override
  protected void append(LoggingEvent loggingEvent) {
    try {
      String message = subAppend(loggingEvent);
      ListenableFuture<List<byte[]>> chiResp = cs.tsWrite(key.getBytes(), message.getBytes());
      Futures.addCallback(chiResp, new FutureCallback<List<byte[]>>() {
        @Override
        public void onSuccess(@Nullable List<byte[]> bytes) {

        }

        @Override
        public void onFailure(Throwable throwable) {
          // TODO(JR): Maybe Try again?
        }
      });
    } catch (ChicagoClientTimeoutException e) {
      e.printStackTrace();
    } catch (ChicagoClientException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void close() {
    try {
      cs.stop();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private String subAppend(LoggingEvent event) {
    return (this.layout == null) ? event.getRenderedMessage() : this.layout.format(event);
  }

  @Override
  public boolean requiresLayout() {
    return true;
  }
}
