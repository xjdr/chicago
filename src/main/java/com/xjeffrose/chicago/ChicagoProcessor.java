package com.xjeffrose.chicago;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
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
  public ListenableFuture<Boolean> process(ChannelHandlerContext ctx, Object req, RequestContext reqCtx) {
    ListeningExecutorService service = MoreExecutors.listeningDecorator(ctx.executor());
    ChicagoMessage msg = null;

    if (req instanceof ChicagoMessage) {
      msg = (ChicagoMessage) req;
    }

    ChicagoMessage finalMsg = msg;
    return service.submit(() -> {

      if (finalMsg == null) {
        reqCtx.setContextData(reqCtx.getConnectionId(), new DefaultChicagoMessage(Op.fromInt(3), "x".getBytes(), Boolean.toString(false).getBytes(), "x".getBytes()));

        return false;
      }

      byte[] readResponse = null;
      boolean status = false;

      switch (finalMsg.getOp()) {
        case READ:
          readResponse = dbManager.read(finalMsg.getKey());
          if (readResponse != null) {
            status = true;
          }
          break;
        case WRITE:
          status = dbManager.write(finalMsg.getKey(), finalMsg.getVal());
          break;
        case DELETE:
          status = dbManager.delete(finalMsg.getKey());
        default:
          break;
      }

      reqCtx.setContextData(reqCtx.getConnectionId(), new DefaultChicagoMessage(Op.fromInt(3), "x".getBytes(), Boolean.toString(status).getBytes(), readResponse) );
      return true;
    });
  }
}
