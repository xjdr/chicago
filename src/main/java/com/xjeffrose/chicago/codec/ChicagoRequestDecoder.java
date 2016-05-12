package com.xjeffrose.chicago.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.bytes.ByteArrayDecoder;
import java.util.List;

public class ChicagoRequestDecoder extends ByteArrayDecoder {

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
    byte[] array = new byte[msg.readableBytes()];
    msg.getBytes(0, array);
    out.add(array);
  }
}
