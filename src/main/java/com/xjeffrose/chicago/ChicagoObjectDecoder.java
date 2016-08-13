package com.xjeffrose.chicago;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Ints;
import com.xjeffrose.chicago.server.Chicago;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.DecoderResult;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import lombok.extern.slf4j.Slf4j;

/*
 * | id | op | ColFam | keySize | key | valSize | val |
 *
 */

@Slf4j
public class ChicagoObjectDecoder extends ByteToMessageDecoder {

  public ChicagoMessage decode(byte[] msg) {
    return _decode(Unpooled.wrappedBuffer(msg));
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
    // Populate the output List

    while (msg.readableBytes() > 50) {
      out.add(_decode(msg));
    }

    if (out.size() > 1 && ((ChicagoMessage)out.get(0)).getOp() == Op.STREAM_RESPONSE ) {
      if (out.get(0) instanceof ChicagoMessage) {
        ChicagoMessage chiMsg = (ChicagoMessage) out.get(0);
        byte[] lastOffset = null;
        UUID id = chiMsg.getId();
        Op op = chiMsg.getOp();
        byte[] colFam = chiMsg.getColFam();
        byte[] key = chiMsg.getKey();
        ByteBuf bb = Unpooled.buffer();

        for (Object cm : out) {
          lastOffset = ((ChicagoMessage)cm).getKey();
          bb.writeBytes(((ChicagoMessage)cm).getVal());
          bb.writeBytes(new byte[]{'\0'});
        }

        bb.writeBytes(ChiUtil.delimiter.getBytes());
        bb.writeBytes(lastOffset);
        out.clear();
        byte[] val = bb.array();
        ChicagoMessage cm = new DefaultChicagoMessage(id,op,colFam,Boolean.toString(true).getBytes(),bb.array());
        cm.setDecoderResult(DecoderResult.SUCCESS);
        out.add(cm);
      }
    }
  }

  private ChicagoMessage _decode(ByteBuf msg) {
    //    final byte[] hash = new byte[4];
    final byte[] id = new byte[36];
    final byte[] op = new byte[4];
    final byte[] colFamSize = new byte[4];
    final byte[] keySize = new byte[4];
    final byte[] valSize = new byte[4];

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
    log.debug("val size = "+ valLength);

    // Get the Value
    msg.readBytes(val, 0, valLength);

    try {
      DefaultChicagoMessage _msg = new DefaultChicagoMessage(UUID.fromString(new String(id)), Op.fromInt(Ints.fromByteArray(op)), colFam, key, val);
      _msg.setDecoderResult(DecoderResult.SUCCESS);
      return _msg;
    } catch (IllegalArgumentException e) {
      log.error("Failure during Decode: ", e);
      DefaultChicagoMessage _msg = new DefaultChicagoMessage(null, Op.fromInt(Ints.fromByteArray(op)), colFam, key, val);
      _msg.setDecoderResult(DecoderResult.failure(e));
      return _msg;
    }
  }
}
