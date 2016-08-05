package com.xjeffrose.chicago.client;

import com.google.common.util.concurrent.SettableFuture;
import com.xjeffrose.chicago.ChicagoMessage;
import com.xjeffrose.xio.core.XioIdleDisconnectException;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ChannelHandler.Sharable
class ChicagoClientHandler extends SimpleChannelInboundHandler<ChicagoMessage> {
  private static final Logger log = LoggerFactory.getLogger(ChicagoClientHandler.class);

  private Map<UUID, SettableFuture<byte[]>> futureMap;
  public ChicagoClientHandler(Map<UUID, SettableFuture<byte[]>> futureMap) {
    this.futureMap = futureMap;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    if (cause.getClass() == XioIdleDisconnectException.class) {
    } else {
      cause.printStackTrace();
    }
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ChicagoMessage chicagoMessage) throws Exception {
    if (chicagoMessage != null) {
      if (futureMap.containsKey(chicagoMessage.getId())) {
        //System.out.println("Got response for ID: "+ chicagoMessage.getId() + " response ="+ Longs.fromByteArray(
        //chicagoMessage.getVal()) + "ctx "+ ctx.toString());
        if (chicagoMessage.getSuccess()) {
          futureMap.get(chicagoMessage.getId()).set(chicagoMessage.getVal());
        } else {
          futureMap.get(chicagoMessage.getId()).setException(new ChicagoClientException("Request Failed"));
        }
        futureMap.remove(chicagoMessage.getId());
      } else {
        //TODO(JR): What to do with a request without map
      }
    } else {
      log.error("Recieved Null response from server for: " + ctx);
    }
  }
}
