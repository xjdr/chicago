package com.xjeffrose.chicago;

import com.google.common.primitives.Ints;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.DecoderResult;
import java.util.List;

public class ChicagoObjectDecoder extends ByteToMessageDecoder {

  public ChicagoMessage decode(byte[] msg) {
    return _decode(Unpooled.wrappedBuffer(msg));
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
    // Populate the output List
    out.add(_decode(msg));
  }

  private ChicagoMessage _decode(ByteBuf msg) {
    final byte[] op = new byte[4];
    final byte[] keyLengthArray = new byte[4];
    final byte[] valLengthArray = new byte[4];

    // Determine the operation type
    msg.readBytes(op, 0, op.length);

    // Get the Key Length
    msg.readBytes(keyLengthArray, 0, keyLengthArray.length);
    final int keyLength = Ints.fromByteArray(keyLengthArray);
    final byte[] key = new byte[keyLength];

    // Get the Key
    msg.readBytes(key, 0, keyLength);

    // Get the Value Length
    msg.readBytes(valLengthArray, 0, valLengthArray.length);
    final int valLength = Ints.fromByteArray(valLengthArray);
    final byte[] val = new byte[valLength];

    // Get the Value
    msg.readBytes(val, 0, valLength);

    DefaultChicagoMessage _msg = new DefaultChicagoMessage(Op.fromInt((int) op[0]), key, val);
    _msg.setDecoderResult(DecoderResult.SUCCESS);

    return _msg;
  }
}
