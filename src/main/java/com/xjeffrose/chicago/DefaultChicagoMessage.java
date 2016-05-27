package com.xjeffrose.chicago;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.DecoderResult;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultChicagoMessage implements ChicagoMessage {
  private static final Logger log = LoggerFactory.getLogger(DefaultChicagoMessage.class.getName());
  private final UUID id;
  private final Op _op;
  private final byte[] colFam;
  private final byte[] key;
  private final byte[] val;
  private DecoderResult decoderResult;

  public DefaultChicagoMessage(UUID id, Op _op, byte[] colFam, byte[] key, byte[] val) {
    this.id = id;
    this._op = _op;
    this.colFam = colFam;
    this.key = key;
    this.val = val;
  }

  public ByteBuf encode() {
    return Unpooled.directBuffer().writeBytes(new ChicagoObjectEncoder().encode(id, _op, colFam, key, val));
  }

  @Override
  public DecoderResult decoderResult() {
    return decoderResult;
  }

  @Override
  public void setDecoderResult(DecoderResult decoderResult) {
    this.decoderResult = decoderResult;
  }

  @Override
  public UUID getId() {
    return id;
  }

  @Override
  public Op getOp() {
    return _op;
  }

  @Override
  public byte[] getKey() {
    return key;
  }

  @Override
  public byte[] getVal() {
    return val;
  }

  @Override
  public boolean getSuccess() {
    return true;
  }

  @Override
  public byte[] getColFam() {
    return colFam;
  }

  @Override
  public String toString() {
    return "id: " + id + " op: " + _op + " key: " + new String(key) + " value: " + new String(val);
  }
}
