package com.xjeffrose.chicago.client;

public class ChicagoClientException extends Exception {
  public ChicagoClientException(Exception requestException) {
    super(requestException);
  }
}
