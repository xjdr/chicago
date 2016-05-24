package com.xjeffrose.chicago;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.xjeffrose.xio.processor.XioProcessor;
import com.xjeffrose.xio.server.RequestContext;
import io.netty.channel.ChannelHandlerContext;
import java.util.UUID;

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
        reqCtx.setContextData(reqCtx.getConnectionId(), new DefaultChicagoMessage(UUID.randomUUID(), Op.fromInt(3), "x".getBytes(), Boolean.toString(false).getBytes(), "x".getBytes()));

        return false;
      }

      byte[] readResponse = null;
      boolean status = false;

      switch (finalMsg.getOp()) {
        case READ:
          readResponse = dbManager.read(finalMsg.getColFam(), finalMsg.getKey());
          if (readResponse != null) {
            status = true;
          }
          reqCtx.setContextData(reqCtx.getConnectionId(), new DefaultChicagoMessage(finalMsg.getId(), Op.fromInt(3), finalMsg.getColFam(), Boolean.toString(status).getBytes(), readResponse));
          break;
        case WRITE:
          status = dbManager.write(finalMsg.getColFam(), finalMsg.getKey(), finalMsg.getVal());
          reqCtx.setContextData(reqCtx.getConnectionId(), new DefaultChicagoMessage(finalMsg.getId(), Op.fromInt(3), finalMsg.getColFam(), Boolean.toString(status).getBytes(), null));
          break;
        case DELETE:
          status = dbManager.delete(finalMsg.getColFam(), finalMsg.getKey());
          reqCtx.setContextData(reqCtx.getConnectionId(), new DefaultChicagoMessage(finalMsg.getId(), Op.fromInt(3), finalMsg.getColFam(), Boolean.toString(status).getBytes(), null));
          break;
        case STREAM:
          readResponse = dbManager.streamFrom(finalMsg.getColFam(), finalMsg.getKey());
          if (readResponse != null) {
            status = true;
          }
          reqCtx.setContextData(reqCtx.getConnectionId(), new DefaultChicagoMessage(finalMsg.getId(), Op.fromInt(5), finalMsg.getColFam(), Boolean.toString(status).getBytes(), readResponse));
        default:
          break;
      }

      return true;
    });
  }
}
