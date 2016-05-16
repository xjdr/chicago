package com.xjeffrose.chicago.client;

import com.xjeffrose.chicago.ChicagoMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

class ChicagoClientHandler extends SimpleChannelInboundHandler<ChicagoMessage> {
  private Listener<byte[]> listener;

  public ChicagoClientHandler(Listener<byte[]> listener) {
    this.listener = listener;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ChicagoMessage chicagoMessage) throws Exception {
    listener.onResponseReceived(chicagoMessage.getVal(), chicagoMessage.getSuccess());
  }
}
