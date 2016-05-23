package com.xjeffrose.chicago.client;


import com.xjeffrose.chicago.ChicagoMessage;
import java.util.UUID;

interface Listener<T> {

  void onRequestSent();

  void onResponseReceived(T message, boolean success);

  void onChannelError(Exception requestException) throws ChicagoClientException;

  T getResponse(UUID id) throws ChicagoClientTimeoutException;

  boolean getStatus(UUID id) throws ChicagoClientTimeoutException;

  void onChannelReadComplete();

  void addID(UUID id);

  void onResponseReceived(ChicagoMessage chicagoMessage);
}
