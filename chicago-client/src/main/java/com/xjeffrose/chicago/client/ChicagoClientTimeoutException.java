package com.xjeffrose.chicago.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

public class ChicagoClientTimeoutException extends Exception {
  private static final Logger log = LoggerFactory.getLogger(ChicagoClientTimeoutException.class.getName());

  public ChicagoClientTimeoutException() {
  }

  public ChicagoClientTimeoutException(TimeoutException e) {
    super(e);
  }
}
