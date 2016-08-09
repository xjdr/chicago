package com.xjeffrose.chicago.server;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.xjeffrose.chicago.ChicagoMessage;
import com.xjeffrose.chicago.DefaultChicagoMessage;
import com.xjeffrose.chicago.Op;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.internal.PlatformDependent;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

// This is a state machine to manage offsets for our Time Series Database functionality
// in our streaming functionality

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

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ChicagoMessage msg) throws Exception {
    switch (msg.getOp()) {
      case GET_OFFSET:
        // The ColFam Exists
        if (offset.containsKey(new String(msg.getColFam()))) {
          // This offset has been written to all members of the replica set
          if (qCount.get(new String(msg.getColFam())).get() == q.get(new String(msg.getColFam()))) {
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
                Longs.toByteArray(offset.get(new String(msg.getColFam())).incrementAndGet())));
            qCount.get(new String(msg.getColFam())).incrementAndGet();
            sessionCoordinator.get(new String(msg.getColFam())).put(new String(msg.getKey()), offset.get(new String(msg.getColFam())).get());

          } else {
            // Return current offset to member of the replica set
            ctx.writeAndFlush(new DefaultChicagoMessage(
                msg.getId(),
                Op.RESPONSE,
                msg.getColFam(),
                null,
                Longs.toByteArray(offset.get(new String(msg.getColFam())).get())));
            qCount.get(new String(msg.getColFam())).incrementAndGet();

          }



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
              Longs.toByteArray(offset.get(new String(msg.getColFam())).get())));
          qCount.get(new String(msg.getColFam())).incrementAndGet();
          sessionCoordinator.get(new String(msg.getColFam())).put(new String(msg.getKey()), offset.get(new String(msg.getColFam())).get());

        }
        break;

      default:
        break;
    }
  }
}
