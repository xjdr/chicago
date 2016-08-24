package com.xjeffrose.chicago;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class ChicagoMessageAggregator extends MessageToMessageDecoder<ChicagoMessage> {
  private final AtomicBoolean started = new AtomicBoolean();
  private AggregatedChicagoMessage aggregatedMessage;

  private void aggregate(ChicagoMessage msg) {
    if (started.get()) {
      aggregatedMessage.appendVal(msg.getVal());
    } else {
      aggregatedMessage = new AggregatedChicagoMessage(msg);
      started.set(true);
    }
  }


  @Override
  protected void decode(ChannelHandlerContext ctx, ChicagoMessage msg, List<Object> out) throws Exception {

    switch (msg.getOp()) {
      case STREAM_RESPONSE:
        aggregate(msg);
        if (new String(msg.getVal()).contains(ChiUtil.delimiter)) {
          out.add(aggregatedMessage);
          started.set(false);
        }

        break;
      default:
        out.add(msg);
    }

  }
}
