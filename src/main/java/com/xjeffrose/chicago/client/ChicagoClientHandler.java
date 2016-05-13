package com.xjeffrose.chicago.client;

import com.xjeffrose.chicago.ChicagoMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class ChicagoClientHandler extends SimpleChannelInboundHandler {
  private Listener<byte[]> listener;

  public ChicagoClientHandler(Listener<byte[]> listener) {
    this.listener = listener;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, Object o) throws Exception {

    if (o instanceof ChicagoMessage) {
      ChicagoMessage chicagoMessage = (ChicagoMessage) o;
      listener.onResponseReceived(chicagoMessage.getVal(), chicagoMessage.getSuccess());
    }


  }
}
