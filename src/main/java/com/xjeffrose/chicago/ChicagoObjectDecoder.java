package com.xjeffrose.chicago;

import com.google.common.primitives.Ints;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.DecoderResult;
import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

/*
 * | id | op | ColFam | keySize | key | valSize | val |
 *
 */

@Slf4j
public class ChicagoObjectDecoder extends ByteToMessageDecoder {

  private static final int MINIMUM_CHICAGO_MESSAGE_SIZE = 50;

  public ChicagoMessage decode(byte[] msg) {
    return decode(Unpooled.wrappedBuffer(msg));
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
    // Populate the output List

    ChicagoMessage chicagoMessage;
    while ((chicagoMessage = decode(msg)) != null) {
      out.add(chicagoMessage);
    }
  }

  private ChicagoMessage decode(ByteBuf msg) {
    if (msg.readableBytes() < MINIMUM_CHICAGO_MESSAGE_SIZE) {
      return null;
    }
    int startingPosition = msg.readerIndex();
    //    final byte[] hash = new byte[4];
    final byte[] id = new byte[36];
    final byte[] op = new byte[4];
    final byte[] colFamSize = new byte[4];
    final byte[] keySize = new byte[4];
    final byte[] valSize = new byte[4];

    // Determine the message ID
    if (msg.readableBytes() < id.length) {
      return null;
    }
    msg.readBytes(id, 0, id.length);

    // Determine the operation type
    if (msg.readableBytes() < op.length) {
      msg.readerIndex(startingPosition);
      return null;
    }
    msg.readBytes(op, 0, op.length);

    // Get the colFam Length
    if (msg.readableBytes() < colFamSize.length) {
      msg.readerIndex(startingPosition);
      return null;
    }
    msg.readBytes(colFamSize, 0, colFamSize.length);
    final int colFamLength = Ints.fromByteArray(colFamSize);
    final byte[] colFam = new byte[colFamLength];

    // Get the Col Fam
    if (msg.readableBytes() < colFam.length) {
      msg.readerIndex(startingPosition);
      return null;
    }
    msg.readBytes(colFam);

    // Get the Key Length
    if (msg.readableBytes() < keySize.length) {
      msg.readerIndex(startingPosition);
      return null;
    }
    msg.readBytes(keySize);
    final int keyLength = Ints.fromByteArray(keySize);
    final byte[] key = new byte[keyLength];

    // Get the Key
    if (msg.readableBytes() < key.length) {
      msg.readerIndex(startingPosition);
      return null;
    }
    msg.readBytes(key);

    // Get the Value Length
    if (msg.readableBytes() < valSize.length) {
      msg.readerIndex(startingPosition);
      return null;
    }
    msg.readBytes(valSize);

    final int valLength = Ints.fromByteArray(valSize);
    final byte[] val = new byte[valLength];
    log.debug("val size = " + valLength);

    // Get the Value
    if (msg.readableBytes() < val.length) {
      msg.readerIndex(startingPosition);
      return null;
    }
    msg.readBytes(val);

    try {
      DefaultChicagoMessage _msg = new DefaultChicagoMessage(UUID.fromString(new String(id)),
        Op.fromInt(Ints.fromByteArray(op)), colFam, key, val);
      _msg.setDecoderResult(DecoderResult.SUCCESS);
      return _msg;
    } catch (IllegalArgumentException e) {
      log.error("Failure during Decode: ", e);
      DefaultChicagoMessage _msg =
        new DefaultChicagoMessage(null, Op.fromInt(Ints.fromByteArray(op)), colFam, key, val);
      _msg.setDecoderResult(DecoderResult.failure(e));
      return _msg;
    }
  }
}
