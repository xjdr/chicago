package com.xjeffrose.chicago.client;

import java.util.Arrays;
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
    if (responseList.size() > 0) {
      if (successList.removeFirst()) {
        return responseList.removeFirst();
      } else if (responseList.size() == 3 && successList.size() == 0) {
        log.error("There were no successful read requests");
        return null;
      }
    } else {
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return getResponse();
    }
    return getResponse();
  }

  @Override
  public boolean getStatus() {
    if (successList.size() == 3) {
      return successList.stream().allMatch(b -> b);
    } else {
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return getStatus();
    }
  }
}
