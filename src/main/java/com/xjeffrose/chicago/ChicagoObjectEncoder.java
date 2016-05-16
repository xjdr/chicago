package com.xjeffrose.chicago;

import com.google.common.hash.Hashing;
import com.google.common.primitives.Ints;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import java.util.List;
import org.apache.log4j.Logger;

public class ChicagoObjectEncoder extends MessageToMessageEncoder<Object> {
  private static final Logger log = Logger.getLogger(ChicagoObjectEncoder.class);


  public ChicagoObjectEncoder() {
  }

  public byte[] encode(Op _op, byte[] key, byte[] val) {

    if (val == null) {
      val = new byte[0];
    }

    byte[] op = Ints.toByteArray(_op.getOp());
    byte[] keySize = Ints.toByteArray(key.length);
    byte[] valSize = Ints.toByteArray(val.length);
    byte[] msgArray = new byte[op.length + keySize.length + key.length + valSize.length + val.length];

    System.arraycopy(op, 0, msgArray, 0, op.length);
    System.arraycopy(keySize, 0, msgArray, op.length, keySize.length);
    System.arraycopy(key, 0, msgArray, op.length + keySize.length, key.length);
    System.arraycopy(valSize, 0, msgArray, op.length + keySize.length + key.length , valSize.length);
    System.arraycopy(val, 0, msgArray, op.length + keySize.length + key.length + valSize.length, val.length);

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

    ByteBuf _msg = ctx.alloc().directBuffer().writeBytes(encode(chiMessage.getOp(), chiMessage.getKey(), chiMessage.getVal()));
    out.add(_msg);
  }
}
