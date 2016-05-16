package com.xjeffrose.chicago.client;

import io.netty.channel.CombinedChannelDuplexHandler;

class ChicagoClientCodec extends CombinedChannelDuplexHandler<ChicagoResponseDecoder, ChicagoRequestEncoder> {

  ChicagoClientCodec() {
    super(new ChicagoResponseDecoder(), new ChicagoRequestEncoder());
  }
}
