package com.xjeffrose.chicago.client;

import java.util.concurrent.TimeoutException;

public class ChicagoClientTimeoutException extends Exception {

  public ChicagoClientTimeoutException() {
  }

  public ChicagoClientTimeoutException(TimeoutException e) {
    super(e);
  }
}
