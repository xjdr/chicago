package com.xjeffrose.chicago;

import com.google.common.hash.Hashing;
import com.google.common.primitives.Ints;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
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

  public ByteBuf encode(ChicagoMessage msg) {
    return encode(msg.getId(), msg.getOp(), msg.getColFam(), msg.getKey(), msg.getVal());
  }

  public ByteBuf encode(ChannelHandlerContext ctx, ChicagoMessage msg) {
    return encode(ctx, msg.getId(), msg.getOp(), msg.getColFam(), msg.getKey(), msg.getVal());
  }

  public ByteBuf encode(UUID _id, Op _op, byte[] colFam, byte[] key, byte[] val) {
    return encode(null, _id, _op, colFam, key, val);
  }

  public ByteBuf encode(ChannelHandlerContext ctx, UUID _id, Op _op, byte[] colFam, byte[] key, byte[] val) {

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

    ByteBuf bb;
    if (ctx == null ) {
      bb = Unpooled.buffer();
    } else {
      bb = ctx.alloc().directBuffer();
    }

    bb.writeBytes(id);
    bb.writeBytes(op);
    bb.writeBytes(colFamSize);
    bb.writeBytes(colFam);
    bb.writeBytes(keySize);
    bb.writeBytes(key);
    bb.writeBytes(valSize);
    bb.writeBytes(val);

    return bb;
  }

  @Override
  protected void encode(ChannelHandlerContext ctx, Object msg, List<Object> out) throws Exception {
    if (msg instanceof ChicagoMessage) {
      out.add(encode(ctx, (ChicagoMessage) msg));
    } else if (msg instanceof ByteBuf) {
      ReferenceCountUtil.retain(msg);
      out.add(msg);
    } else {
      log.error("Object not an instance of ChicagoMessage: " + msg);
    }
  }
}
