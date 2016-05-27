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
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/*
 * | checksum | id | op | ColFam | keySize | key | valSize | val |
 *
 */


public class ChicagoObjectDecoder extends ByteToMessageDecoder {
  private static final Logger log = LoggerFactory.getLogger(ChicagoObjectDecoder.class.getName());


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
    final byte[] id = new byte[36];
    final byte[] op = new byte[4];
    final byte[] colFamSize = new byte[4];
    final byte[] keySize = new byte[4];
    final byte[] valSize = new byte[4];

    // Determine the operation type
    msg.readBytes(hash, 0, hash.length);
    HashCode hashCode = HashCode.fromBytes(hash);

    // Determine the message ID
    msg.readBytes(id, 0, id.length);

    // Determine the operation type
    msg.readBytes(op, 0, op.length);

    // Get the colFam Length
    msg.readBytes(colFamSize, 0, colFamSize.length);
    final int colFamLength = Ints.fromByteArray(colFamSize);
    final byte[] colFam = new byte[colFamLength];

    // Get the Col Fam
    msg.readBytes(colFam, 0, colFam.length);

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

    DefaultChicagoMessage _msg = null;

    try {
      _msg = new DefaultChicagoMessage(UUID.fromString(new String(id)), Op.fromInt(Ints.fromByteArray(op)), colFam, key, val);
    } catch (IllegalArgumentException e) {
      log.error("Failure during Decode: ", e);
      _msg = new DefaultChicagoMessage(null, Op.fromInt(Ints.fromByteArray(op)), colFam, key, val);
    }

    int msgSize = id.length + op.length + colFamSize.length + colFam.length + keySize.length + key.length + valSize.length + val.length;
    byte[] msgArray = new byte[msgSize];

    int trailing = 0;
    System.arraycopy(id, 0, msgArray, trailing, id.length);
    trailing = trailing + id.length;
    System.arraycopy(op, 0, msgArray, trailing, op.length);
    trailing = trailing + op.length;
    System.arraycopy(colFamSize, 0, msgArray, trailing, colFamSize.length);
    trailing = trailing + colFamSize.length;
    System.arraycopy(colFam, 0, msgArray, trailing, colFam.length);
    trailing = trailing + colFam.length;
    System.arraycopy(keySize, 0, msgArray, trailing, keySize.length);
    trailing = trailing + keySize.length;
    System.arraycopy(key, 0, msgArray, trailing, key.length);
    trailing = trailing + key.length;
    System.arraycopy(valSize, 0, msgArray, trailing, valSize.length);
    trailing = trailing + valSize.length;
    System.arraycopy(val, 0, msgArray, trailing, val.length);

    HashCode messageHash = Hashing.murmur3_32().hashBytes(msgArray);

    if (hashCode.equals(messageHash)) {
      _msg.setDecoderResult(DecoderResult.SUCCESS);
    } else {
      _msg.setDecoderResult(DecoderResult.failure(new ChicagoDecodeException()));
    }

    return _msg;
  }
}
