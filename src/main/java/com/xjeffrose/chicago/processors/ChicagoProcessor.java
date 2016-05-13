package com.xjeffrose.chicago.processors;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
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

  private byte[] createResponse(boolean status) {
    byte[] response = new byte[12];

    return response;
  }

  @Override
  public ListenableFuture<Boolean> process(ChannelHandlerContext ctx, Object request, RequestContext reqCtx) {
    ListeningExecutorService service = MoreExecutors.listeningDecorator(ctx.executor());

    return service.submit(() -> {
      reqCtx.setContextData(reqCtx.getConnectionId(), request);
      return true;
    });
  }
}
