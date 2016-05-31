package com.xjeffrose.chicago;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.xjeffrose.xio.processor.XioProcessor;
import com.xjeffrose.xio.processor.XioSimpleProcessor;
import com.xjeffrose.xio.server.RequestContext;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChicagoProcessor implements XioSimpleProcessor {
  private static final Logger log = LoggerFactory.getLogger(ChicagoProcessor.class);

  @Override
  public void disconnect(ChannelHandlerContext channelHandlerContext) {

  }

  @Override
  public ListenableFuture<Boolean> process(ChannelHandlerContext ctx, Object req, RequestContext requestContext) {
    final ListeningExecutorService service = MoreExecutors.listeningDecorator(ctx.executor());

    return service.submit(() -> {
      if (log.isDebugEnabled()) {
        ChicagoMessage msg = null;

    if (req instanceof ChicagoMessage) {
      msg = (ChicagoMessage) req;
    }

    ChicagoMessage finalMsg = msg;
        log.debug("  ========================================================== Server wrote :" +
            " For UUID" + finalMsg.getId() + " and key " + new String(finalMsg.getKey()));
      }
      return true;
    });
  }
//
//  private DBManager dbManager;
//
//  public ChicagoProcessor(DBManager dbManager) {
//    this.dbManager = dbManager;
//  }
//
//  @Override
//  public void disconnect(ChannelHandlerContext ctx) {
//
//  }
//
//  @Override
//  public ListenableFuture<Boolean> process(ChannelHandlerContext ctx, Object req, RequestContext reqCtx) {
//    ListeningExecutorService service = MoreExecutors.listeningDecorator(ctx.executor());
//    ChicagoMessage msg = null;
//
//    if (req instanceof ChicagoMessage) {
//      msg = (ChicagoMessage) req;
//    }
//
//    ChicagoMessage finalMsg = msg;
//    return service.submit(() -> {
//
//      if (finalMsg == null) {
////        reqCtx.setContextData(reqCtx.getConnectionId(), new DefaultChicagoMessage(Op.fromInt(3), "x".getBytes(), Boolean.toString(false).getBytes(), "x".getBytes()));
//
//        return false;
//      }
//
//      byte[] readResponse = null;
//      boolean status = false;
//
//      switch (finalMsg.getOp()) {
//        case READ:
//          readResponse = dbManager.read(finalMsg.getColFam(), finalMsg.getKey());
//          if (readResponse != null) {
//            status = true;
//          }
//          break;
//        case WRITE:
//          status = dbManager.write(finalMsg.getColFam(), finalMsg.getKey(), finalMsg.getVal());
//          log.debug("  ========================================================== Server wrote :" +
//              status + " For UUID" + finalMsg.getId() + " and key " + new String(finalMsg.getKey()));
//          break;
//        case DELETE:
//          status = dbManager.delete(finalMsg.getColFam(), finalMsg.getKey());
//          break;
//        case TS_WRITE:
//          readResponse = dbManager.tsWrite(finalMsg.getColFam(), finalMsg.getVal());
//          if (readResponse != null) {
//            status = true;
//          }
//          break;
//        case STREAM:
//          readResponse = dbManager.stream(finalMsg.getColFam(), finalMsg.getVal());
//          if (readResponse != null) {
//            status = true;
//          }
//          break;
//        default:
//          break;
//      }
//
//      reqCtx.setContextData(reqCtx.getConnectionId(), new DefaultChicagoMessage(finalMsg.getId(), Op.fromInt(3), finalMsg.getColFam(), Boolean.toString(status).getBytes(), readResponse));
////        ctx.writeAndFlush(new DefaultChicagoMessage(finalMsg.getId(), Op.fromInt(3), finalMsg.getColFam(), Boolean.toString(status).getBytes(), readResponse));
//
//      return true;
//    });
//  }
}
