package com.xjeffrose.chicago.client;

import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChicagoClientTimeoutException extends Exception {
  private static final Logger log = LoggerFactory.getLogger(ChicagoClientTimeoutException.class.getName());

  public ChicagoClientTimeoutException() {
  }

  public ChicagoClientTimeoutException(TimeoutException e) {
    super(e);
  }
}
