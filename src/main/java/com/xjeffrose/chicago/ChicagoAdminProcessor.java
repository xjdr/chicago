package com.xjeffrose.chicago;

import com.google.common.util.concurrent.ListenableFuture;
import com.xjeffrose.xio.processor.XioProcessor;
import com.xjeffrose.xio.server.RequestContext;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChicagoAdminProcessor implements XioProcessor {
  private static final Logger log = LoggerFactory.getLogger(ChicagoAdminProcessor.class.getName());
  @Override
  public void disconnect(ChannelHandlerContext channelHandlerContext) {

  }

  @Override
  public ListenableFuture<Boolean> process(ChannelHandlerContext channelHandlerContext, Object o, RequestContext requestContext) {
    return null;
  }
}
