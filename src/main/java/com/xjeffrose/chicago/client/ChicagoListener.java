package com.xjeffrose.chicago.client;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;

class ChicagoListener implements Listener<byte[]> {
  private static final Logger log = Logger.getLogger(ChicagoListener.class);
  private static final long TIMEOUT = 1000;


  private final AtomicInteger statusRefNumber = new AtomicInteger();
  private final Map<Integer, Boolean> statusMap = new ConcurrentHashMap<>(); 
  private final AtomicInteger responseRefNumber = new AtomicInteger();

  private final Map<Integer, Map<byte[], Boolean>> responseMap = new ConcurrentHashMap<>();

  private final  AtomicInteger currentStatus = new AtomicInteger();
  private final AtomicInteger currentResponse = new AtomicInteger();

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
    return _getResponse(System.currentTimeMillis());
  }

  private byte[] _getResponse(long startTime) throws ChicagoClientTimeoutException {
    while (responseMap.isEmpty()) {
      if ((System.currentTimeMillis() - startTime) > TIMEOUT) {
        Thread.currentThread().interrupt();
        throw new ChicagoClientTimeoutException();
      }
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    while (!responseMap.containsKey(currentResponse.get())) {
      if ((System.currentTimeMillis() - startTime) > TIMEOUT) {
        Thread.currentThread().interrupt();
        throw new ChicagoClientTimeoutException();
      }
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    Map<byte[], Boolean> _resp = responseMap.get(currentResponse.getAndIncrement());


      byte[] resp = (byte[]) _resp.keySet().toArray()[0];

    if (_resp.get(resp)) {
      return resp;
    } else {
      log.error("Invalid Response returned");
      return null;
    }

  }


  @Override
  public boolean getStatus() throws ChicagoClientTimeoutException {
    return _getStatus(System.currentTimeMillis());
  }

  private boolean _getStatus(long startTime) throws ChicagoClientTimeoutException {
    while (statusMap.isEmpty()) {
      if ((System.currentTimeMillis() - startTime) > TIMEOUT) {
        Thread.currentThread().interrupt();
        throw new ChicagoClientTimeoutException();
      }
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    while (!statusMap.containsKey(currentStatus.get())) {
      if ((System.currentTimeMillis() - startTime) > TIMEOUT) {
        Thread.currentThread().interrupt();
        throw new ChicagoClientTimeoutException();
      }
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    boolean resp = statusMap.get(currentStatus.getAndIncrement());

    return resp;
  }


  @Override
  public void onChannelReadComplete() {

  }


}
