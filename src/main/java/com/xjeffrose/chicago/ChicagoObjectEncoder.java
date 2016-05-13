package com.xjeffrose.chicago;

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
    byte[] op = {(byte) _op.getOp()};
    byte[] keySize = {(byte) key.length};
    byte[] valSize ={(byte) val.length};
    byte[] msgArray = new byte[op.length + keySize.length + key.length + valSize.length + val.length];

    System.arraycopy(op, 0, msgArray, 0, op.length);
    System.arraycopy(keySize, 0, msgArray, op.length, keySize.length);
    System.arraycopy(key, 0, msgArray, op.length + keySize.length, key.length);
    System.arraycopy(valSize, 0, msgArray, op.length + keySize.length + key.length , valSize.length);
    System.arraycopy(val, 0, msgArray, op.length + keySize.length + key.length + valSize.length, val.length);

    return msgArray;
  }

  @Override
  protected void encode(ChannelHandlerContext ctx, Object msg, List<Object> out) throws Exception {
    ChicagoMessage chiMessage = null;

    if (msg instanceof ChicagoMessage) {
      chiMessage = (ChicagoMessage) msg;
    } else {
      log.error("Object not an instance of ChicagoMessage: " + msg);
    }

    byte[] key = chiMessage.getKey();
    byte[] val = chiMessage.getVal();
    byte[] op = {(byte) chiMessage.getOp().getOp()};
    byte[] keySize = {(byte) key.length};
    byte[] valSize ={(byte) (val != null ? val.length : 0)};
    byte[] msgArray = new byte[op.length + keySize.length + key.length + valSize.length + (val != null ? val.length : 0)];

    System.arraycopy(op, 0, msgArray, 0, op.length);
    System.arraycopy(keySize, 0, msgArray, op.length, keySize.length);
    System.arraycopy(key, 0, msgArray, op.length + keySize.length, key.length);
    System.arraycopy(valSize, 0, msgArray, op.length + keySize.length + key.length , valSize.length);
    System.arraycopy((val != null ? val : new byte[]{0}), 0, msgArray, op.length + keySize.length + key.length + valSize.length, (val != null ? val.length : 0));

    ByteBuf _msg = ctx.alloc().directBuffer().writeBytes(msgArray);
    out.add(_msg);
  }
}
