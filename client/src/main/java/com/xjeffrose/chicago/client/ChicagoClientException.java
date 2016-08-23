package com.xjeffrose.chicago.client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChicagoClientException extends Exception {
  private static final Logger log = LoggerFactory.getLogger(ChicagoClientException.class.getName());

  public ChicagoClientException(Exception requestException) {
    super(requestException);
  }

  public ChicagoClientException(String s) {
    super(s);
  }
}
