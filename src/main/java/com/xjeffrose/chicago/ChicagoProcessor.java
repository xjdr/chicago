package com.xjeffrose.chicago;

import com.google.common.util.concurrent.ListenableFuture;
import com.xjeffrose.xio.processor.XioProcessor;
import com.xjeffrose.xio.server.RequestContext;
import io.netty.channel.ChannelHandlerContext;

public class ChicagoProcessor implements XioProcessor {
  @Override
  public void disconnect(ChannelHandlerContext ctx) {

  }

  @Override
  public ListenableFuture<Boolean> process(ChannelHandlerContext ctx, Object o, RequestContext requestContext) {
    return null;
  }
}
