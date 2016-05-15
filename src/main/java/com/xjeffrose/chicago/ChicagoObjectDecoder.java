package com.xjeffrose.chicago;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
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
    final byte[] hash = new byte[4];
    final byte[] op = new byte[4];
    final byte[] keySize = new byte[4];
    final byte[] valSize = new byte[4];

    // Determine the operation type
    msg.readBytes(hash, 0, hash.length);
    HashCode hashCode = HashCode.fromBytes(hash);

    // Determine the operation type
    msg.readBytes(op, 0, op.length);

    // Get the Key Length
    msg.readBytes(keySize, 0, keySize.length);
    final int keyLength = Ints.fromByteArray(keySize);
    final byte[] key = new byte[keyLength];

    // Get the Key
    msg.readBytes(key, 0, keyLength);

    // Get the Value Length
    msg.readBytes(valSize, 0, valSize.length);
    final int valLength = Ints.fromByteArray(valSize);
    final byte[] val = new byte[valLength];

    // Get the Value
    msg.readBytes(val, 0, valLength);

    DefaultChicagoMessage _msg = new DefaultChicagoMessage(Op.fromInt(Ints.fromByteArray(op)), key, val);

    byte[] msgArray = new byte[op.length + keySize.length + key.length + valSize.length + val.length];

    System.arraycopy(op, 0, msgArray, 0, op.length);
    System.arraycopy(keySize, 0, msgArray, op.length, keySize.length);
    System.arraycopy(key, 0, msgArray, op.length + keySize.length, key.length);
    System.arraycopy(valSize, 0, msgArray, op.length + keySize.length + key.length , valSize.length);
    System.arraycopy(val, 0, msgArray, op.length + keySize.length + key.length + valSize.length, val.length);

    HashCode messageHash = Hashing.murmur3_32().hashBytes(msgArray);
    
    if (hashCode.equals(messageHash) ) {
      _msg.setDecoderResult(DecoderResult.SUCCESS);
    } else {
      _msg.setDecoderResult(DecoderResult.failure(new ChicagoDecodeException()));
    }

    return _msg;
  }
}
