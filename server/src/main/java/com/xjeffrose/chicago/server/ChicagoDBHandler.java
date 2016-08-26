package com.xjeffrose.chicago.server;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.xjeffrose.chicago.ChiUtil;
import com.xjeffrose.chicago.ChicagoMessage;
import com.xjeffrose.chicago.ChicagoObjectEncoder;
import com.xjeffrose.chicago.ChicagoPaxosClient;
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
import io.netty.util.internal.PlatformDependent;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ChannelHandler.Sharable
public class ChicagoDBHandler extends SimpleChannelInboundHandler<ChicagoMessage> {
  private final DBManager db;
  private final ChicagoPaxosClient paxosClient;
  private final Map<String, AtomicLong> offset = PlatformDependent.newConcurrentHashMap();
  private final Map<String, Integer> q = PlatformDependent.newConcurrentHashMap();
  private final Map<String, Map<String, Long>> sessionCoordinator = PlatformDependent.newConcurrentHashMap();
  private final Map<String, AtomicInteger> qCount = PlatformDependent.newConcurrentHashMap();


  public ChicagoDBHandler(DBManager db, ChicagoPaxosClient paxosClient) {
    this.db = db;
    this.paxosClient = paxosClient;
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
    if (msg.getKey().length == 0) {
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
        for (int i = 0; i < result.size(); i++) {
          DBRecord record = result.get(i);
          if (i == result.size() - 1) {
            ByteBuf lastval = Unpooled.buffer();
            lastval.writeBytes(record.getValue());
            lastval.writeBytes(ChiUtil.delimiter.getBytes());
            lastval.writeBytes(record.getKey());
            record.setValue(lastval.array());
          }
          bb.writeBytes(encoder.encode(new DefaultChicagoMessage(msg.getId(), Op.STREAM_RESPONSE, msg.getColFam(), Boolean.toString(true).getBytes(), record.getValue())));
        }
        ctx.writeAndFlush(bb).addListener(writeComplete);
      }

      @Override
      public void onFailure(Throwable error) {
      }
    }, ctx.executor());
  }

  private void handleScanKeys(ChannelHandlerContext ctx, ChicagoMessage msg, ChannelFutureListener writeComplete) {
    ListenableFuture<List<byte[]>> future = db.getKeys(msg.getColFam());
    Futures.addCallback(future, new FutureCallback<List<byte[]>>() {
      @Override
      public void onSuccess(List<byte[]> result) {
        ByteBuf bb = Unpooled.buffer();
        for (byte[] record : result) {
          String temp = new String(record) + ChiUtil.delimiter;
          bb.writeBytes(temp.getBytes());
        }
        ctx.writeAndFlush(
            new DefaultChicagoMessage(
                msg.getId(),
                Op.RESPONSE,
                msg.getColFam(),
                Boolean.toString(true).getBytes(),
                bb.array()
            )
        ).addListener(writeComplete);
      }

      @Override
      public void onFailure(Throwable error) {
      }
    }, ctx.executor());
  }

  private void handlePaxosPropose(ChannelHandlerContext ctx, ChicagoMessage msg, ChannelFutureListener writeComplete) {

  }

  private void handlePaxosPromise(ChannelHandlerContext ctx, ChicagoMessage msg, ChannelFutureListener writeComplete) {

  }

  private void handlePaxosAccept(ChannelHandlerContext ctx, ChicagoMessage msg, ChannelFutureListener writeComplete) {

  }

  private void handleMPaxosProposeWrite(ChannelHandlerContext ctx, ChicagoMessage msg, ChannelFutureListener writeComplete) {
    handleWrite(ctx, msg, writeComplete);
  }

  private void handleMPaxosProposeTsWrite(ChannelHandlerContext ctx, ChicagoMessage msg, ChannelFutureListener writeComplete) {
    handleTimeSeriesWrite(ctx, msg, writeComplete);
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
        ListenableFuture<List<byte[]>> f = paxosClient.write(msg.getColFam(), msg.getKey(), msg.getVal());
        Futures.addCallback(f, new FutureCallback<List<byte[]>>() {
          @Override
          public void onSuccess(@Nullable List<byte[]> bytes) {
            if (bytes.size() >= ( paxosClient.getReplicaSize() - 1)) {
              handleWrite(ctx, msg, writeComplete);
            }
          }

          @Override
          public void onFailure(Throwable throwable) {
            // TODO(JR): Create a reasonable failure response
          }
        });
      case DELETE:
        handleDelete(ctx, msg, writeComplete);
        break;
      case TS_WRITE:
        ListenableFuture<List<byte[]>> ff = paxosClient.tsWrite(msg.getColFam(), msg.getKey(), msg.getVal());
        Futures.addCallback(ff, new FutureCallback<List<byte[]>>() {
          @Override
          public void onSuccess(@Nullable List<byte[]> bytes) {
            if (bytes.size() >= ( paxosClient.getReplicaSize() - 1)) {
              handleTimeSeriesWrite(ctx, msg, writeComplete);
            }
          }

          @Override
          public void onFailure(Throwable throwable) {
            // TODO(JR): Create a reasonable failure response
            log.error("I'm a bad request .... ");
          }
        });
        break;
      case STREAM:
        handleStreamingRead(ctx, msg, writeComplete);
        break;
      case GET_OFFSET:
        handleGettingOffset(ctx, msg, writeComplete);
        break;
      case SCAN_KEYS:
        handleScanKeys(ctx, msg, writeComplete);
        break;
      case PAXOS_PROPOSE:
        handlePaxosPropose(ctx, msg, writeComplete);
        break;
      case PAXOS_PROMISE:
        handlePaxosPromise(ctx, msg, writeComplete);
        break;
      case PAXOS_ACCEPT:
        handlePaxosAccept(ctx, msg, writeComplete);
        break;
      case MPAXOS_PROPOSE_WRITE:
        handleMPaxosProposeWrite(ctx, msg, writeComplete);
        break;
      case MPAXOS_PROPOSE_TS_WRITE:
        handleMPaxosProposeTsWrite(ctx, msg, writeComplete);
        break;
      default:
        break;
    }
  }

  private void handleGettingOffset(ChannelHandlerContext ctx, ChicagoMessage msg, ChannelFutureListener writeComplete) {
    // The ColFam Exists
    if (offset.containsKey(new String(msg.getColFam()))) {
      // This offset has been written to all members of the replica set
//      if (qCount.get(new String(msg.getColFam())).get() == q.get(new String(msg.getColFam()))) {
        if (sessionCoordinator.containsKey(new String(msg.getColFam()))) {

        } else {
          sessionCoordinator.put(new String(msg.getColFam()), PlatformDependent.newConcurrentHashMap());
          sessionCoordinator.get(new String(msg.getKey())).put(new String(msg.getKey()), offset.get(new String(msg.getColFam())).incrementAndGet());
        }

        ctx.writeAndFlush(new DefaultChicagoMessage(
            msg.getId(),
            Op.RESPONSE,
            msg.getColFam(),
            null,
            Longs.toByteArray(offset.get(new String(msg.getColFam())).incrementAndGet()))).addListener(writeComplete);
        qCount.get(new String(msg.getColFam())).incrementAndGet();
        sessionCoordinator.get(new String(msg.getColFam())).put(new String(msg.getKey()), offset.get(new String(msg.getColFam())).get());

//      } else {
//        // Return current offset to member of the replica set
//        ctx.writeAndFlush(new DefaultChicagoMessage(
//            msg.getId(),
//            Op.RESPONSE,
//            msg.getColFam(),
//            null,
//            Longs.toByteArray(offset.get(new String(msg.getColFam())).get()))).addListener(writeComplete);
//        qCount.get(new String(msg.getColFam())).incrementAndGet();
//
//      }


    } else {
      // Create the offset for the ColFam on first message (ColFam Create)
      offset.put(new String(msg.getColFam()), new AtomicLong());
      q.put(new String(msg.getColFam()), Ints.fromByteArray(msg.getVal()));
      qCount.put(new String(msg.getColFam()), new AtomicInteger());
      sessionCoordinator.put(new String(msg.getColFam()), PlatformDependent.newConcurrentHashMap());

      ctx.writeAndFlush(new DefaultChicagoMessage(
          msg.getId(),
          Op.RESPONSE,
          msg.getColFam(),
          null,
          Longs.toByteArray(offset.get(new String(msg.getColFam())).get()))).addListener(writeComplete);
      qCount.get(new String(msg.getColFam())).incrementAndGet();
      sessionCoordinator.get(new String(msg.getColFam())).put(new String(msg.getKey()), offset.get(new String(msg.getColFam())).get());
    }
  }


}
