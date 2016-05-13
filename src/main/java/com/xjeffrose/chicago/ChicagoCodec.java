package com.xjeffrose.chicago;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.CombinedChannelDuplexHandler;
import io.netty.handler.codec.http.HttpServerUpgradeHandler;

public class ChicagoCodec extends CombinedChannelDuplexHandler<ChicagoObjectDecoder, ChicagoObjectEncoder> implements HttpServerUpgradeHandler.SourceCodec {

  public ChicagoCodec() {
    super(new ChicagoObjectDecoder(), new ChicagoObjectEncoder());
  }

  @Override
  public void upgradeFrom(ChannelHandlerContext channelHandlerContext) {

  }
}
