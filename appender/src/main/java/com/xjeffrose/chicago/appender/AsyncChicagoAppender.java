package com.xjeffrose.chicago.appender;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.xjeffrose.chicago.client.ChicagoAsyncClient;
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
  private ChicagoAsyncClient cs;

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
      cs = new ChicagoAsyncClient(chicagoZk, 3);
      cs.start();
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Cannot instantiate a client");
    }

    LogLog.debug("Chicago connected to " + chicagoZk);
  }

  @Override
  protected void append(LoggingEvent loggingEvent) {
    try {
      String message = subAppend(loggingEvent);
      ListenableFuture<byte[]> chiResp = cs.tsWrite(key.getBytes(), message.getBytes());
      if (chiResp != null) {
        Futures.addCallback(chiResp, new FutureCallback<byte[]>() {
          @Override
          public void onSuccess(@Nullable byte[] bytes) {

          }

          @Override
          public void onFailure(Throwable throwable) {
            // TODO(JR): Maybe Try again?
          }
        });
      } else {
        //Todo : Maybe try again since the future was null.
      }
    } catch (Exception e){
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    try {
      cs.close();
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
