package com.xjeffrose.chicago.processors;

import com.google.common.util.concurrent.ListenableFuture;
import com.xjeffrose.chicago.DBManager;
import com.xjeffrose.xio.processor.XioProcessor;
import com.xjeffrose.xio.server.RequestContext;
import io.netty.channel.ChannelHandlerContext;

public class ChicagoProcessor implements XioProcessor {
  private DBManager dbManager;

  public ChicagoProcessor(DBManager dbManager) {
    this.dbManager = dbManager;
  }

  @Override
  public void disconnect(ChannelHandlerContext ctx) {

  }

  @Override
  public ListenableFuture<Boolean> process(ChannelHandlerContext ctx, Object o, RequestContext requestContext) {
    return null;
  }
}
