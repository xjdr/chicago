package com.xjeffrose.chicago.processors;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.bytes.ByteArrayDecoder;
import java.util.List;

public class ChicagoRequestDecoder extends ByteArrayDecoder {

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
    final byte[] op = new byte[1];
    final byte[] keyLengthArray = new byte[1];
    final byte[] valLengthArray = new byte[1];

    // Determine the operation type
    msg.getBytes(0, op, 0, op.length);

    // Get the Key Length
    msg.getBytes(1, keyLengthArray, 0, keyLengthArray.length);
    final int keyLength = keyLengthArray[0];
    final byte[] key = new byte[keyLength];

    // Get the Key
    msg.getBytes(op.length + keyLengthArray.length, key, 0, keyLength);

    // Get the Value Length
    msg.getBytes(2 + keyLength, valLengthArray, 0, 1);
    final int valLength = valLengthArray[0];
    final byte[] val = new byte[valLength];

    // Get the Value
    msg.getBytes(keyLength + 3, val, 0, valLength);

    // Populate the output List
    out.add((int) op[0]);
    out.add(key);
    out.add(val);
  }
}
