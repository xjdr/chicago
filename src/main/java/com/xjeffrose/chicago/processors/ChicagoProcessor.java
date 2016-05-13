package com.xjeffrose.chicago.processors;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.xjeffrose.chicago.DBManager;
import com.xjeffrose.chicago.Op;
import com.xjeffrose.xio.processor.XioProcessor;
import com.xjeffrose.xio.server.RequestContext;
import io.netty.channel.ChannelHandlerContext;
import java.util.List;

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
  public ListenableFuture<Boolean> process(ChannelHandlerContext ctx, Object req, RequestContext reqCtx) {
    ListeningExecutorService service = MoreExecutors.listeningDecorator(ctx.executor());

    List<Object> reqList = (List<Object>) req;
    Op op = Op.fromInt((int) reqList.get(0));

    return service.submit(() -> {

      if (op == null) {
        reqCtx.setContextData(reqCtx.getConnectionId(), createResponse(false));
        return false;
      }

      switch (op) {
        case READ:
          dbManager.read((byte[]) reqList.get(1));
          break;
        case WRITE:
          dbManager.write((byte[]) reqList.get(1), (byte[]) reqList.get(2));
          break;
        case DELETE:
          dbManager.delete((byte[]) reqList.get(1));
        default:
          break;
      }

      reqCtx.setContextData(reqCtx.getConnectionId(), createResponse(true));
      return true;
    });
  }
}
