package com.xjeffrose.chicago.server;

import com.xjeffrose.chicago.ChiUtil;
import com.xjeffrose.chicago.ChicagoMessage;
import com.xjeffrose.chicago.ChicagoObjectEncoder;
import com.xjeffrose.chicago.DefaultChicagoMessage;
import com.xjeffrose.chicago.Op;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ChannelHandler.Sharable
public class ChicagoDBHandler extends SimpleChannelInboundHandler<ChicagoMessage> {
  private static final Logger log = LoggerFactory.getLogger(ChicagoDBHandler.class);

  private final DBInterface db;
  private final ChicagoObjectEncoder encoder = new ChicagoObjectEncoder();

  public ChicagoDBHandler(DBInterface db, DBLog dbLog) {
    this.db = db;
  }

  private ChicagoMessage createErrorMessage() {
    return new DefaultChicagoMessage(UUID.randomUUID(), Op.fromInt(3), "x".getBytes(), Boolean.toString(false).getBytes(), "x".getBytes());
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

  private void handleRead(ChannelHandlerContext ctx, ChicagoMessage msg, ChannelFutureListener writeComplete) {
    ctx.writeAndFlush(
      new DefaultChicagoMessage(
        msg.getId(),
        Op.RESPONSE,
        msg.getColFam(),
        Boolean.toString(true).getBytes(),
        db.read(msg.getColFam(), msg.getKey())
      )
    ).addListener(writeComplete);
  }

  private void handleWrite(ChannelHandlerContext ctx, ChicagoMessage msg, ChannelFutureListener writeComplete) {
    ctx.writeAndFlush(
      new DefaultChicagoMessage(
        msg.getId(),
        Op.RESPONSE,
        msg.getColFam(),
        Boolean.toString(db.write(msg.getColFam(), msg.getKey(), encoder.encode(msg))).getBytes(),
        null
      )
    ).addListener(writeComplete);
  }

  private void handleDelete(ChannelHandlerContext ctx, ChicagoMessage msg, ChannelFutureListener writeComplete) {
    if (msg.getKey().length == 0) {
      db.delete(msg.getColFam());
    } else {
      db.delete(msg.getColFam(), msg.getKey());
    }
  }

  private void handleTimeSeriesWrite(ChannelHandlerContext ctx, ChicagoMessage msg, ChannelFutureListener writeComplete) {
    if (msg.getKey().length == 0) {
      if (new String(msg.getVal()).contains(ChiUtil.delimiter)) {
        ctx.writeAndFlush(
          new DefaultChicagoMessage(
            msg.getId(),
            Op.RESPONSE,
            msg.getColFam(),
            Boolean.toString(true).getBytes(),
            db.batchWrite(msg.getColFam(), msg.getVal())
          )
        ).addListener(writeComplete);
      } else {
        ctx.writeAndFlush(
          new DefaultChicagoMessage(
            msg.getId(),
            Op.RESPONSE,
            msg.getColFam(),
            Boolean.toString(true).getBytes(),
            db.tsWrite(msg.getColFam(), encoder.encode(msg))
          )
        ).addListener(writeComplete);
      }
    } else {
      ctx.writeAndFlush(
        new DefaultChicagoMessage(
          msg.getId(),
          Op.RESPONSE,
          msg.getColFam(),
          Boolean.toString(true).getBytes(),
          db.tsWrite(msg.getColFam(), msg.getKey(), encoder.encode(msg))
        )
      ).addListener(writeComplete);
    }
  }

  private void handleStreamingRead(ChannelHandlerContext ctx, ChicagoMessage msg, ChannelFutureListener writeComplete) {
    ctx.writeAndFlush(
      new DefaultChicagoMessage(
        msg.getId(),
        Op.RESPONSE,
        msg.getColFam(),
        Boolean.toString(true).getBytes(),
        db.stream(msg.getColFam(), msg.getVal())
      )
    ).addListener(writeComplete);
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ChicagoMessage msg) throws Exception {
    ChannelFutureListener writeComplete = new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) {
        if (!future.isSuccess()) {
          log.error("Server error writing :" + " For UUID" + msg.getId() + " and key " + new String(msg.getKey()));
        }
      }
    };

    switch (msg.getOp()) {
      case READ:
        handleRead(ctx, msg, writeComplete);
        break;
      case WRITE:
        handleWrite(ctx, msg, writeComplete);
        break;
      case DELETE:
        handleDelete(ctx, msg, writeComplete);
        break;
      case TS_WRITE:
        handleTimeSeriesWrite(ctx, msg, writeComplete);
        break;
      case STREAM:
        handleStreamingRead(ctx, msg, writeComplete);
        break;

      default:
        break;
    }
  }
}
