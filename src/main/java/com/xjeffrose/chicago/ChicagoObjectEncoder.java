package com.xjeffrose.chicago;

import com.google.common.hash.Hashing;
import com.google.common.primitives.Ints;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * | id | op | ColFam | keySize | key | valSize | val |
 *
 *
 */

public class ChicagoObjectEncoder extends MessageToMessageEncoder<Object> {
  private static final Logger log = LoggerFactory.getLogger(ChicagoObjectEncoder.class);


  public ChicagoObjectEncoder() {
  }

  public byte[] encode(UUID _id, Op _op, byte[] colFam, byte[] key, byte[] val) {

    if (key == null) {
      key = new byte[0];
    }

    if (val == null) {
      val = new byte[0];
    }

    byte[] id = _id.toString().getBytes();
    byte[] op = Ints.toByteArray(_op.getOp());
    byte[] colFamSize = Ints.toByteArray(colFam.length);
    byte[] keySize = Ints.toByteArray(key.length);
    byte[] valSize = Ints.toByteArray(val.length);
    int msgSize = id.length + op.length + colFamSize.length + colFam.length + keySize.length + key.length + valSize.length + val.length;
    byte[] msgArray = new byte[msgSize];

    int trailing = 0;
    System.arraycopy(id, 0, msgArray, trailing, id.length);
    trailing = trailing + id.length;
    System.arraycopy(op, 0, msgArray, trailing, op.length);
    trailing = trailing + op.length;
    System.arraycopy(colFamSize, 0, msgArray, trailing, colFamSize.length );
    trailing = trailing + colFamSize.length;
    System.arraycopy(colFam, 0, msgArray, trailing, colFam.length );
    trailing = trailing + colFam.length;
    System.arraycopy(keySize, 0, msgArray, trailing, keySize.length);
    trailing = trailing + keySize.length;
    System.arraycopy(key, 0, msgArray, trailing, key.length);
    trailing = trailing + key.length;
    System.arraycopy(valSize, 0, msgArray, trailing , valSize.length);
    trailing = trailing + valSize.length;
    System.arraycopy(val, 0, msgArray, trailing, val.length);

    byte[] hash = Hashing.murmur3_32().hashBytes(msgArray).asBytes();

    byte[] hashedMessage = new byte[hash.length + msgArray.length];
    System.arraycopy(hash, 0, hashedMessage, 0, hash.length);
    System.arraycopy(msgArray, 0, hashedMessage, hash.length, msgArray.length);

    return hashedMessage;
  }

  @Override
  protected void encode(ChannelHandlerContext ctx, Object msg, List<Object> out) throws Exception {
    ChicagoMessage chiMessage = null;

    if (msg instanceof ChicagoMessage) {
      chiMessage = (ChicagoMessage) msg;
    } else {
      log.error("Object not an instance of ChicagoMessage: " + msg);
    }

    ByteBuf _msg = ctx.alloc().directBuffer().writeBytes(encode(chiMessage.getId(), chiMessage.getOp(), chiMessage.getColFam(), chiMessage.getKey(), chiMessage.getVal()));
    out.add(_msg);
  }
}
