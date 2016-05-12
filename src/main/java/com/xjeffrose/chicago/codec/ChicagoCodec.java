package com.xjeffrose.chicago.codec;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.CombinedChannelDuplexHandler;
import io.netty.handler.codec.http.HttpServerUpgradeHandler;

public class ChicagoCodec extends CombinedChannelDuplexHandler<ChicagoRequestDecoder, ChicagoResponseEncoder> implements HttpServerUpgradeHandler.SourceCodec {

  public ChicagoCodec() {
    super(new ChicagoRequestDecoder(), new ChicagoResponseEncoder());
  }

  @Override
  public void upgradeFrom(ChannelHandlerContext channelHandlerContext) {

  }
}
