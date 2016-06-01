package com.xjeffrose.chicago;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelFuture;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChicagoDBHandler extends SimpleChannelInboundHandler {
  private static final Logger log = LoggerFactory.getLogger(ChicagoDBHandler.class);
  private static final int MAX_BUFFER_SIZE = 16000;

  private final DBManager dbManager;

  private boolean needsToWrite = false;
  private byte[] readResponse = null;
  private boolean status = false;
  private ChicagoMessage finalMsg = null;
  private final DBLog dbLog;


  public ChicagoDBHandler(DBManager dbManager, DBLog dbLog) {
    this.dbManager = dbManager;
    this.dbLog = dbLog;
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
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    log.debug("Connection Active for: " + ctx.channel().localAddress());
    log.debug("Connection Active for: " + ctx.channel().remoteAddress());

    ctx.fireChannelActive();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    log.debug("Connection InActive for: " + ctx.channel().localAddress());
    log.debug("Connection InActive for: " + ctx.channel().remoteAddress());

    ctx.fireChannelInactive();
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
          dbLog.addRead(finalMsg.getColFam(), finalMsg.getKey());

          if (readResponse != null) {
            status = true;
          }
          break;
        case WRITE:
          status = dbManager.write(finalMsg.getColFam(), finalMsg.getKey(), finalMsg.getVal());
          dbLog.addWrite(finalMsg.getColFam(), finalMsg.getKey(), finalMsg.getVal());
          log.debug("  ========================================================== Server wrote :" +
              status + " For UUID" + finalMsg.getId() + " and key " + new String(finalMsg.getKey()));
          break;
        case DELETE:
          status = dbManager.delete(finalMsg.getColFam(), finalMsg.getKey());
          dbLog.addDelete(finalMsg.getColFam(), finalMsg.getKey());
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

        if (readResponse != null && readResponse.length > MAX_BUFFER_SIZE) {
          ByteBuf bb = Unpooled.buffer();
          bb.writeBytes(readResponse);

          for (int i = 0; i < Math.ceil(bb.readableBytes() / MAX_BUFFER_SIZE) ; i++) {
            ByteBuf bbs = bb.slice(MAX_BUFFER_SIZE * i, MAX_BUFFER_SIZE);
            ctx.writeAndFlush(new DefaultChicagoMessage(finalMsg.getId(), Op.fromInt(3), finalMsg.getColFam(), Boolean.toString(status).getBytes(), bbs.array()));
          }

          readResponse = new byte[]{};
        }

        break;

      default:
        break;
      }

      ChannelFutureListener writeComplete = new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture future) {
            if (!future.isSuccess()) {
              log.error("Server error writing :" +
              status + " For UUID" + finalMsg.getId() + " and key " + new String(finalMsg.getKey()));
            }
          }
        };
      needsToWrite = true;
      ctx.writeAndFlush(new DefaultChicagoMessage(finalMsg.getId(), Op.fromInt(3), finalMsg.getColFam(), Boolean.toString(status).getBytes(), readResponse)).addListener(writeComplete);
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
