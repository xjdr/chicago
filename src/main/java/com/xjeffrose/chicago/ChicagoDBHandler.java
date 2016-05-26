package com.xjeffrose.chicago;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.util.UUID;
import org.apache.log4j.Logger;

public class ChicagoDBHandler extends SimpleChannelInboundHandler {
  private static final Logger log = Logger.getLogger(ChicagoDBHandler.class);

  private final DBManager dbManager;

  private boolean needsToWrite = false;
  private byte[] readResponse = null;
  private boolean status = false;
  private ChicagoMessage finalMsg = null;


  public ChicagoDBHandler(DBManager dbManager) {
    this.dbManager = dbManager;
  }

  private ChicagoMessage createErrorMessage() {
    return  new DefaultChicagoMessage(UUID.randomUUID(), Op.fromInt(3), "x".getBytes(), Boolean.toString(false).getBytes(), "x".getBytes());
  }


  private ChicagoMessage createMessage() {
    if (finalMsg == null) {
      return createErrorMessage();
    }
    return new DefaultChicagoMessage(finalMsg.getId(), Op.fromInt(3), finalMsg.getColFam(), Boolean.toString(status).getBytes(), readResponse);
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, Object req) throws Exception {
    ChicagoMessage msg = null;

    if (req instanceof ChicagoMessage) {
      msg = (ChicagoMessage) req;
    }

    finalMsg = msg;

      if (finalMsg == null) {
        needsToWrite = true;
      }

      switch (finalMsg.getOp()) {
        case READ:
          readResponse = dbManager.read(finalMsg.getColFam(), finalMsg.getKey());
          if (readResponse != null) {
            status = true;
          }
          break;
        case WRITE:
          status = dbManager.write(finalMsg.getColFam(), finalMsg.getKey(), finalMsg.getVal());
          log.debug("  ========================================================== Server wrote :" +
              status + " For UUID" + finalMsg.getId() + " and key " + new String(finalMsg.getKey()));
          break;
        case DELETE:
          status = dbManager.delete(finalMsg.getColFam(), finalMsg.getKey());
          break;
        case TS_WRITE:
          readResponse = dbManager.tsWrite(finalMsg.getColFam(), finalMsg.getVal());
          if (readResponse != null) {
            status = true;
          }
          break;
        case STREAM:
          readResponse = dbManager.stream(finalMsg.getColFam(), finalMsg.getVal());
          if (readResponse != null) {
            status = true;
          }
          break;
        default:
          break;
      }

    needsToWrite = true;
    ctx.writeAndFlush(new DefaultChicagoMessage(finalMsg.getId(), Op.fromInt(3), finalMsg.getColFam(), Boolean.toString(status).getBytes(), readResponse));
  }

//  @Override
//  public void channelActive(ChannelHandlerContext ctx) {
//    writeIfPossible(ctx.channel());
//  }
//  @Override
//  public void channelWritabilityChanged(ChannelHandlerContext ctx) {
//    writeIfPossible(ctx.channel());
//  }
//
//  private void writeIfPossible(Channel channel) {
//    while(needsToWrite && channel.isWritable()) {
//      channel.writeAndFlush(createMessage());
//    }
//  }
}
