package com.xjeffrose.chicago.client;

import com.xjeffrose.chicago.ChicagoMessage;
import com.xjeffrose.xio.core.XioIdleDisconnectException;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ChicagoClientHandler extends SimpleChannelInboundHandler<ChicagoMessage> {
  private static final Logger log = LoggerFactory.getLogger(ChicagoClientHandler.class);

  private Listener<byte[]> listener;

  public ChicagoClientHandler(Listener<byte[]> listener) {
    this.listener = listener;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    if (cause.getClass() == XioIdleDisconnectException.class) {

    } else {
      listener.onChannelError((Exception) cause);
    }

//    ctx.fireExceptionCaught(cause);
  }


  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    listener.onChannelReadComplete();
//    ctx.fireChannelReadComplete();
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ChicagoMessage chicagoMessage) throws Exception {
    log.debug("channelRead0 message: " + chicagoMessage);
      listener.onResponseReceived(chicagoMessage);
  }
}
