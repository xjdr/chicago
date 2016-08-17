package com.xjeffrose.chicago.server;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.xjeffrose.chicago.ChiUtil;
import com.xjeffrose.chicago.ChicagoMessage;
import com.xjeffrose.chicago.ChicagoObjectEncoder;
import com.xjeffrose.chicago.DefaultChicagoMessage;
import com.xjeffrose.chicago.Op;
import com.xjeffrose.chicago.db.DBManager;
import com.xjeffrose.chicago.db.DBRecord;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ChannelHandler.Sharable
public class ChicagoDBHandler extends SimpleChannelInboundHandler<ChicagoMessage> {
  private static final Logger log = LoggerFactory.getLogger(ChicagoDBHandler.class);
  private final DBManager db;

  public ChicagoDBHandler(DBManager db) {
    this.db = db;
  }

  private ChicagoMessage createErrorMessage() {
    return new DefaultChicagoMessage(UUID.randomUUID(), Op.RESPONSE, "x".getBytes(), Boolean.toString(false).getBytes(), "x".getBytes());
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
    ListenableFuture<byte[]> future = db.read(msg.getColFam(), msg.getKey());
    Futures.addCallback(future, new FutureCallback<byte[]>() {
      @Override
      public void onSuccess(byte[] result) {
        ctx.writeAndFlush(
          new DefaultChicagoMessage(
            msg.getId(),
            Op.RESPONSE,
            msg.getColFam(),
            Boolean.toString(true).getBytes(),
            result
          )
        ).addListener(writeComplete);
      }
      @Override
      public void onFailure(Throwable error) {
      }
    }, ctx.executor());
  }

  private void handleWrite(ChannelHandlerContext ctx, ChicagoMessage msg, ChannelFutureListener writeComplete) {
    ListenableFuture<Boolean> future = db.write(msg.getColFam(), msg.getKey(), msg.getVal());
    Futures.addCallback(future, new FutureCallback<Boolean>() {
      @Override
      public void onSuccess(Boolean result) {
        ctx.writeAndFlush(
          new DefaultChicagoMessage(
            msg.getId(),
            Op.RESPONSE,
            msg.getColFam(),
            Boolean.toString(result).getBytes(),
            null
          )
        ).addListener(writeComplete);
      }
      @Override
      public void onFailure(Throwable error) {
      }
    }, ctx.executor());
  }

  private void handleDelete(ChannelHandlerContext ctx, ChicagoMessage msg, ChannelFutureListener writeComplete) {
    ListenableFuture<Boolean> future;
    if (msg.getKey().length == 0) {
      future = db.delete(msg.getColFam(), null);
    } else {
      future = db.delete(msg.getColFam(), msg.getKey());
    }
    Futures.addCallback(future, new FutureCallback<Boolean>() {
      @Override
      public void onSuccess(Boolean result) {
      }
      @Override
      public void onFailure(Throwable error) {
      }
    }, ctx.executor());
  }

  private void handleTimeSeriesWrite(ChannelHandlerContext ctx, ChicagoMessage msg, ChannelFutureListener writeComplete) {
    // TODO(CK): batch write should have it's own op
    if (msg.getKey().length == 0) {
      if (new String(msg.getVal()).contains(ChiUtil.delimiter)) {
        ListenableFuture<byte[]> future = db.batchWrite(msg.getColFam(), msg.getVal());
        Futures.addCallback(future, new FutureCallback<byte[]>() {
          @Override
          public void onSuccess(byte[] result) {
            ctx.writeAndFlush(
              new DefaultChicagoMessage(
                msg.getId(),
                Op.RESPONSE,
                msg.getColFam(),
                Boolean.toString(true).getBytes(),
                result
              )
            ).addListener(writeComplete);
          }
          @Override
          public void onFailure(Throwable error) {
          }
        }, ctx.executor());
      } else {
        ListenableFuture<byte[]> future = db.tsWrite(msg.getColFam(), null, msg.getVal());
        Futures.addCallback(future, new FutureCallback<byte[]>() {
          @Override
          public void onSuccess(byte[] result) {
            ctx.writeAndFlush(
              new DefaultChicagoMessage(
                msg.getId(),
                Op.RESPONSE,
                msg.getColFam(),
                Boolean.toString(true).getBytes(),
                result
              )
            ).addListener(writeComplete);
          }
          @Override
          public void onFailure(Throwable error) {
          }
        }, ctx.executor());
      }
    } else {
      ListenableFuture<byte[]> future = db.tsWrite(msg.getColFam(), msg.getKey(), msg.getVal());
      Futures.addCallback(future, new FutureCallback<byte[]>() {
        @Override
        public void onSuccess(byte[] result) {
          ctx.writeAndFlush(
            new DefaultChicagoMessage(
              msg.getId(),
              Op.RESPONSE,
              msg.getColFam(),
              Boolean.toString(true).getBytes(),
              result
            )
          ).addListener(writeComplete);
        }
        @Override
        public void onFailure(Throwable error) {
        }
      }, ctx.executor());
    }
  }

  private void handleStreamingRead(ChannelHandlerContext ctx, ChicagoMessage msg, ChannelFutureListener writeComplete) {
    ListenableFuture<List<DBRecord>> future = db.stream(msg.getColFam(), msg.getVal());
    Futures.addCallback(future, new FutureCallback<List<DBRecord>>() {
      @Override
      public void onSuccess(List<DBRecord> result) {
        ByteBuf bb = Unpooled.buffer();
        ChicagoObjectEncoder encoder = new ChicagoObjectEncoder();
        for(int i =0; i<result.size(); i++){
          DBRecord record = result.get(i);
          if(i == result.size()-1){
            ByteBuf lastval = Unpooled.buffer();
            lastval.writeBytes(record.getValue());
            lastval.writeBytes(ChiUtil.delimiter.getBytes());
            lastval.writeBytes(record.getKey());
            record.setValue(lastval.array());
          }
          bb.writeBytes(encoder.encode(new DefaultChicagoMessage(msg.getId(),Op.STREAM_RESPONSE,msg.getColFam(),Boolean.toString(true).getBytes(),record.getValue())));
        }
        ctx.writeAndFlush(bb).addListener(writeComplete);
      }
      @Override
      public void onFailure(Throwable error) {
      }
    }, ctx.executor());
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
