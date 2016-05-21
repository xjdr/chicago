package com.xjeffrose.chicago.client;

import com.xjeffrose.chicago.ChicagoMessage;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.log4j.Logger;

class ChicagoListener implements Listener<byte[]> {
  private static final Logger log = Logger.getLogger(ChicagoListener.class);
  private static final long TIMEOUT = 1000;

  private final ConcurrentLinkedDeque<UUID> reqIds = new ConcurrentLinkedDeque<>();
  private final ConcurrentLinkedDeque<UUID> messageIds = new ConcurrentLinkedDeque<>();
  private final Map<UUID, ChicagoMessage> responseMap = new ConcurrentHashMap<>();

  public ChicagoListener() {

  }


  @Override
  public void onRequestSent() {
  }

  @Override
  public void onResponseReceived(byte[] message, boolean success) {
  }

  @Override
  public void onResponseReceived(ChicagoMessage chicagoMessage) {
    messageIds.add(chicagoMessage.getId());
    responseMap.put(chicagoMessage.getId(), chicagoMessage);
  }

  @Override
  public void onChannelError(Exception requestException) {
    log.error("Error Reading Response: ", requestException);
  }


  @Override
  public byte[] getResponse(UUID id) throws ChicagoClientTimeoutException {
    return _getResponse(id, System.currentTimeMillis());
  }

  private byte[] _getResponse(UUID id, long startTime) throws ChicagoClientTimeoutException {
    while (Collections.disjoint(reqIds, messageIds)) {
      if ((System.currentTimeMillis() - startTime) > TIMEOUT) {
//        Thread.currentThread().interrupt();
        throw new ChicagoClientTimeoutException();
      }
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    while (!responseMap.containsKey(id)) {
      try {
        if ((System.currentTimeMillis() - startTime) > TIMEOUT) {
//        Thread.currentThread().interrupt();
          throw new ChicagoClientTimeoutException();
        }
        Thread.sleep(1);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    ChicagoMessage _resp = responseMap.get(id);

    if (_resp.getSuccess()) {
      return _resp.getVal();
    } else {
      log.error("Invalid Response returned");
      return null;
    }
  }

  @Override
  public boolean getStatus(UUID id) throws ChicagoClientTimeoutException {
    boolean resp = _getStatus(id, System.currentTimeMillis());
    return resp;
  }

  private boolean _getStatus(UUID id, long startTime) throws ChicagoClientTimeoutException {
    while (Collections.disjoint(reqIds, messageIds)) {
      if ((System.currentTimeMillis() - startTime) > TIMEOUT) {
//        Thread.currentThread().interrupt();
        throw new ChicagoClientTimeoutException();
      }
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    while (!responseMap.containsKey(id)) {
      try {
        if ((System.currentTimeMillis() - startTime) > TIMEOUT) {
//        Thread.currentThread().interrupt();
          throw new ChicagoClientTimeoutException();
        }
        Thread.sleep(1);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    if (responseMap.get(id).getKey().length == 4) {
      return true;
    } else {
      return false;
    }

  }


  @Override
  public void onChannelReadComplete() {

  }

  @Override
  public void addID(UUID id) {
    reqIds.add(id);
  }


}
