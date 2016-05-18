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
        Thread.sleep(1);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return getResponse();
    }

    if (!successList.getFirst()) {
      log.error("READ operation unsuccessful");
      return null;
    }

    return responseList.removeFirst();
  }

  @Override
  public boolean getStatus() {
    //TODO(JR): Add timeout
    if (successList.isEmpty()) {
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return getStatus();
    } else {
      return successList.removeFirst();
    }
  }
}
