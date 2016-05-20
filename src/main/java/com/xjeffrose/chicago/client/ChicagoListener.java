package com.xjeffrose.chicago.client;

import com.google.common.collect.ImmutableMap;
import com.xjeffrose.xio.core.XioTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;

class ChicagoListener implements Listener<byte[]> {
  private static final Logger log = Logger.getLogger(ChicagoListener.class);

  private final AtomicInteger statusRefNumber = new AtomicInteger();
  private final Map<Integer, Boolean> statusMap = new ConcurrentHashMap<>();
  private final AtomicInteger responseRefNumber = new AtomicInteger();

  private final Map<Integer, Map<byte[], Boolean>> responseMap = new ConcurrentHashMap<>();

  int status = 0;
  int response = 0;

  public ChicagoListener() {

  }


  @Override
  public void onRequestSent() {

  }

  @Override
  public void onResponseReceived(byte[] message, boolean success) {
    if (message.length == 0) {
      if (success) {
        statusMap.put(statusRefNumber.getAndIncrement(), success);
      }
    } else if (message.length > 0) {
      responseMap.put(responseRefNumber.getAndIncrement(), ImmutableMap.of(message, success));
    }
  }

  @Override
  public void onChannelError(Exception requestException) {
    log.error("Error Reading Response: ", requestException);
  }


  @Override
  public byte[] getResponse() throws ChicagoClientTimeoutException {
    XioTimer xioTimer = new XioTimer("Client Timeout Timer");

    xioTimer.newTimeout(new TimerTask() {
      @Override
      public void run(Timeout timeout) throws Exception {
        Thread.currentThread().interrupt();
        throw new ChicagoClientTimeoutException();
      }
    }, 500, TimeUnit.MILLISECONDS);

    return _getResponse(xioTimer);
  }


  private byte[] _getResponse(XioTimer xioTimer) {
    if (responseMap.isEmpty()) {
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return _getResponse(xioTimer);
    }

    if (responseMap.containsKey(response)) {
      Map<byte[], Boolean> _respMap = responseMap.get(response);
      response++;

      byte[] _resp = (byte[]) _respMap.keySet().toArray()[0];

      if (_respMap.get(_resp)) {

        xioTimer.stop();
        return _resp;
      } else {
        log.error("AHHHHHHHHH response");
//      throw new ChicagoClientException();
      }
    }

    try {
      Thread.sleep(1);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    return _getResponse(xioTimer);
  }

  @Override
  public boolean getStatus() throws ChicagoClientTimeoutException {
    XioTimer xioTimer = new XioTimer("Client Timeout Timer");

    xioTimer.newTimeout(new TimerTask() {
      @Override
      public void run(Timeout timeout) throws Exception {
        Thread.currentThread().interrupt();
        throw new ChicagoClientTimeoutException();
      }
    }, 500, TimeUnit.MILLISECONDS);
    return _getStatus(xioTimer);
  }

  private boolean _getStatus(XioTimer xioTimer) {
    if (statusMap.isEmpty()) {
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return _getStatus(xioTimer);
    }

    boolean stat;
    if (statusMap.containsKey(status)) {
      stat = statusMap.get(status);
      status++;

      xioTimer.stop();
      return stat;
    }

    try {
      Thread.sleep(1);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return _getStatus(xioTimer);
  }

  @Override
  public void onChannelReadComplete() {
  }


}
