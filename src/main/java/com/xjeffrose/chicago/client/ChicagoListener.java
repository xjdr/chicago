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
  private final XioTimer xioTimer = new XioTimer("Client Timeout Timer");

  int status = 0;
  int response = 0;


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
  public  byte[] getResponse() {
    xioTimer.newTimeout(new TimerTask() {
      @Override
      public void run(Timeout timeout) throws Exception {
        Thread.interrupted();
        throw new ChicagoClientTimeoutException();
      }
    }, 500, TimeUnit.MILLISECONDS);

    return _getResponse();
  }


  private byte[] _getResponse() {
    if (responseMap.isEmpty()) {
      try {
        Thread.sleep(5);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return getResponse();
    }

    if (responseMap.containsKey(response)) {
      Map<byte[], Boolean> _respMap = responseMap.get(response);
      response++;

      byte[] _resp = (byte[]) _respMap.keySet().toArray()[0];

      if (_respMap.get(_resp)) {
        return _resp;
      } else {
        log.error("AHHHHHHHHH response");
//      throw new ChicagoClientException();
      }
    }

    try {
      Thread.sleep(5);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    return getResponse();
  }

  @Override
  public boolean getStatus() {
    xioTimer.newTimeout(new TimerTask() {
      @Override
      public void run(Timeout timeout) throws Exception {
        Thread.interrupted();
        throw new ChicagoClientTimeoutException();
      }
    }, 500, TimeUnit.MILLISECONDS);

    return _getStatus();
  }

  private boolean _getStatus() {
    if (statusMap.isEmpty()) {
      try {
        Thread.sleep(5);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return _getStatus();
    }

    boolean stat;
    if (statusMap.containsKey(status)) {
      stat = statusMap.get(status);
      status++;

      return stat;
    }

    try {
      Thread.sleep(5);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return _getStatus();
  }

  @Override
  public void onChannelReadComplete() {
  }


}
