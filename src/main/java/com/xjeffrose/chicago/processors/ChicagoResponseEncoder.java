package com.xjeffrose.chicago.processors;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.bytes.ByteArrayEncoder;
import java.util.List;

public class ChicagoResponseEncoder extends ByteArrayEncoder {

  @Override
  protected void encode(ChannelHandlerContext ctx, byte[] msg, List<Object> out) throws Exception {
    out.add(Unpooled.wrappedBuffer(msg));
  }
}
