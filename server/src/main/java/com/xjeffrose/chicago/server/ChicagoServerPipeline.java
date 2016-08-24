package com.xjeffrose.chicago.server;

import com.xjeffrose.chicago.ChicagoCodec;
import com.xjeffrose.xio.pipeline.XioTlsServerPipeline;
import io.netty.channel.ChannelHandler;

public class ChicagoServerPipeline extends XioTlsServerPipeline {

  private final String name;

  public ChicagoServerPipeline(String name) {
    this.name = name;
  }

  @Override
  public ChannelHandler getCodecHandler() {
    return new ChicagoCodec();
  }

  @Override
  public String applicationProtocol() {
    return "tls-chicago-" + name;
  }

}
