package com.xjeffrose.chicago;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.CombinedChannelDuplexHandler;
import io.netty.handler.codec.http.HttpServerUpgradeHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChicagoCodec extends CombinedChannelDuplexHandler<ChicagoObjectDecoder, ChicagoObjectEncoder> implements HttpServerUpgradeHandler.SourceCodec {
  private static final Logger log = LoggerFactory.getLogger(ChicagoCodec.class.getName());

  public ChicagoCodec() {
    super(new ChicagoObjectDecoder(), new ChicagoObjectEncoder());
  }

  @Override
  public void upgradeFrom(ChannelHandlerContext channelHandlerContext) {

  }
}
