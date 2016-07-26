package com.xjeffrose.chicago.client;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.xjeffrose.chicago.ChicagoMessage;
import com.xjeffrose.xio.core.XioIdleDisconnectException;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ChicagoClientHandler extends SimpleChannelInboundHandler<ChicagoMessage> {
  private static final Logger log = LoggerFactory.getLogger(ChicagoClientHandler.class);

  private Listener<byte[]> listener;
  private Map<UUID, SettableFuture<byte[]>> futureMap;

  public ChicagoClientHandler(Listener<byte[]> listener) {
    this.listener = listener;
  }

  public ChicagoClientHandler(Map<UUID, SettableFuture<byte[]>> futureMap) {
    this.futureMap = futureMap;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    if (cause.getClass() == XioIdleDisconnectException.class) {

    } else {
//      listener.onChannelError((Exception) cause);
    }

//    ctx.fireExceptionCaught(cause);
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    if(listener != null) {
//      listener.onChannelReadComplete();
    }
//    ctx.fireChannelReadComplete();
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ChicagoMessage chicagoMessage) throws Exception {
//    log.debug("channelRead0 message: " + chicagoMessage);
//      listener.onResponseReceived(chicagoMessage);
    if (futureMap.containsKey(chicagoMessage.getId())) {
      if (chicagoMessage.getSuccess()) {
        futureMap.get(chicagoMessage.getId()).set(chicagoMessage.getVal());
      } else {
        futureMap.get(chicagoMessage.getId()).setException(new ChicagoClientException("Request Failed"));
      }
    } else {
      //TODO(JR): What to do with a request without map
    }
  }
}
