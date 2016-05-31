package com.xjeffrose.chicago.client;

import io.netty.channel.CombinedChannelDuplexHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ChicagoClientCodec extends CombinedChannelDuplexHandler<ChicagoResponseDecoder, ChicagoRequestEncoder> {
  private static final Logger log = LoggerFactory.getLogger(ChicagoClientCodec.class.getName());

  ChicagoClientCodec() {
    super(new ChicagoResponseDecoder(), new ChicagoRequestEncoder());
  }
}
