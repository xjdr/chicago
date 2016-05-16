package com.xjeffrose.chicago.client;


interface Listener<T> {

  void onRequestSent();

  void onResponseReceived(T message, boolean success);

  void onChannelError(Exception requestException);

  T getResponse();

  boolean getStatus();
}
