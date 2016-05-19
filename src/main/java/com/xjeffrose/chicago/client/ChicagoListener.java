package com.xjeffrose.chicago.client;

import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.log4j.Logger;

class ChicagoListener implements Listener<byte[]> {
  private static final Logger log = Logger.getLogger(ChicagoListener.class);

  private ConcurrentLinkedDeque<byte[]> responseList = new ConcurrentLinkedDeque<>();
  private ConcurrentLinkedDeque<Boolean> successList = new ConcurrentLinkedDeque<>();

  @Override
  public void onRequestSent() {

  }

  @Override
  public void onResponseReceived(byte[] message, boolean success) {
    responseList.add(message);
    successList.add(success);
    if (!success) {
      log.error("Unsuccessful request");
    }
  }

  @Override
  public void onChannelError(Exception requestException) {
    log.error("Error Reading Response: ", requestException);
  }

  @Override
  public byte[] getResponse() {
    if (responseList.isEmpty()) {
      try {
        Thread.sleep(5);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return getResponse();
    }

    if (!successList.isEmpty()) {
      if (!successList.getFirst()) {
        log.error("READ operation unsuccessful");
        return null;
      }
    }

    if (successList.size() == 0 && responseList.size() > 0) {
      log.error("WTFF");
      try {
        Thread.sleep(5);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return getResponse();
    }

      return responseList.stream().filter(xs -> xs.length == 0).findFirst().orElseGet(null);
  }

  @Override
  public boolean getStatus() {
    //TODO(JR): Add timeout
    if (successList.isEmpty()) {
      try {
        Thread.sleep(5);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return getStatus();
    } else {
      return successList.removeFirst();
    }
  }

  @Override
  public void onChannelReadComplete() {

  }
}
