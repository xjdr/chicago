package com.xjeffrose.chicago.client;

import com.xjeffrose.chicago.ChicagoMessage;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ChicagoListener implements Listener<byte[]> {
  private static final Logger log = LoggerFactory.getLogger(ChicagoListener.class);

  private static final ConcurrentLinkedDeque<UUID> reqIds = new ConcurrentLinkedDeque<>();
  private static final Map<UUID, ResponseListener> callBackListener = new ConcurrentHashMap<>();

  public ChicagoListener() {
  }

  @Override
  public void registerCallback(UUID uuid,ResponseListener responseListener){
    callBackListener.put(uuid, responseListener);
  }

  @Override
  public void onRequestSent() {
  }

  @Override
  public void onResponseReceived(byte[] message, boolean success) {
  }

  @Override
  public void onResponseReceived(ChicagoMessage chicagoMessage){
  }

  @Override
  public void onResponseReceived(ChicagoMessage chicagoMessage, ChannelHandlerContext ctx) {
    if (chicagoMessage.getId() == null) {
      log.error("Returned null message: " + chicagoMessage);
    }
    ResponseListener responseListener = callBackListener.remove(chicagoMessage.getId());
    responseListener.onResponse(chicagoMessage, ctx);
  }

  @Override
  public void onChannelError(ChannelHandlerContext ctx,Exception requestException) throws ChicagoClientException {
    if(requestException instanceof IOException) {
      log.debug("Error Reading Response: ", requestException);
    }else{
      log.error("Error Reading Response: ", requestException);
      throw new ChicagoClientException(requestException);
    }
  }


  @Override
  public byte[] getResponse(UUID id) throws ChicagoClientTimeoutException {
    return _getResponse(id, System.currentTimeMillis());
  }

  private byte[] _getResponse(UUID id, long startTime) throws ChicagoClientTimeoutException {
    //Todo: Need to delete this from interface and here.
    return null;
  }

  @Override
  public boolean getStatus(UUID id) throws ChicagoClientTimeoutException {
    boolean resp = _getStatus(id, System.currentTimeMillis());
    return resp;
  }

  private boolean _getStatus(UUID id, long startTime) throws ChicagoClientTimeoutException {
    //Todo: Need to delete this from interface and here.
    return true;
  }


  @Override
  public void onChannelReadComplete() {

  }

  @Override
  public void addID(UUID id) {
    reqIds.add(id);
  }

  @Override
  public void removeID(UUID id) {
    reqIds.remove(id);
  }

  @Override
  public ConcurrentLinkedDeque<UUID> getReqIds() {
    return reqIds;
  }

  @Override
  public byte[] getResponse(ConcurrentLinkedDeque<UUID> idList) throws ChicagoClientTimeoutException {
    return _getResponse(idList, System.currentTimeMillis());
  }

  private byte[] _getResponse(ConcurrentLinkedDeque<UUID> idList, long startTime) throws ChicagoClientTimeoutException {
    //Todo: Need to delete this from interface and here.
    return null;
  }

  @Override
  public byte[] getStatus(ConcurrentLinkedDeque<UUID> idList) throws ChicagoClientTimeoutException {
    return _getStatus(idList, System.currentTimeMillis());
  }

  private byte[] _getStatus(ConcurrentLinkedDeque<UUID> idList, long startTime) throws ChicagoClientTimeoutException {
    //Todo: Need to delete this from interface and here.
    return null;
  }
}
