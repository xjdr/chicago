package com.xjeffrose.chicago.server;

import com.xjeffrose.chicago.ChicagoMessage;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ChannelHandler.Sharable
public class ChicagoPaxosHandler extends SimpleChannelInboundHandler<ChicagoMessage> {
  private final Map<String, AtomicLong> offset;
  private final Map<String, Integer> q;
  private final Map<String, Map<String, Long>> sessionCoordinator;
  private final Map<String, AtomicInteger> qCount;

  public ChicagoPaxosHandler(Map<String, AtomicLong> offset,
                             Map<String, Integer> q,
                             Map<String, Map<String, Long>> sessionCoordinator,
                             Map<String, AtomicInteger> qCount) {

    this.offset = offset;
    this.q = q;
    this.sessionCoordinator = sessionCoordinator;
    this.qCount = qCount;
  }

  private void handlePaxosPropose(ChannelHandlerContext ctx, ChicagoMessage msg, ChannelFutureListener writeComplete) {

  }

  private void handlePaxosPromise(ChannelHandlerContext ctx, ChicagoMessage msg, ChannelFutureListener writeComplete) {

  }

  private void handlePaxosAccept(ChannelHandlerContext ctx, ChicagoMessage msg, ChannelFutureListener writeComplete) {

  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ChicagoMessage msg) throws Exception {    ChannelFutureListener writeComplete = new ChannelFutureListener() {
    @Override
    public void operationComplete(ChannelFuture future) {
      if (!future.isSuccess()) {
        log.error("Server error writing :" + " For UUID" + msg.getId() + " and key " + new String(msg.getKey()));
      }
    }
  };

    switch (msg.getOp()) {
      case PAXOS_PROPOSE:
        handlePaxosPropose(ctx, msg, writeComplete);
        break;
      case PAXOS_PROMISE:
        handlePaxosPromise(ctx, msg, writeComplete);
        break;
      case PAXOS_ACCEPT:
        handlePaxosAccept(ctx, msg, writeComplete);

      default:
        break;
    }
  }
}
