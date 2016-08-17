package com.xjeffrose.chicago;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.DecoderResult;
import java.util.UUID;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@EqualsAndHashCode(exclude={})
//@ToString(exclude={})
public class AggregatedChicagoMessage implements ChicagoMessage {
  private final UUID id;
  private final Op _op;
  private final byte[] colFam;
  private final byte[] key;

  private byte[] val;
  private DecoderResult decoderResult;


  public AggregatedChicagoMessage(ChicagoMessage msg) {
    this(msg.getId(), msg.getOp(), msg.getColFam(), Boolean.toString(true).getBytes(), msg.getVal());
  }

  public AggregatedChicagoMessage(UUID id, Op _op, byte[] colFam, byte[] key, byte[] val) {
    this.id = id;
    this._op = _op;
    this.colFam = colFam;
    this.key = key;
    this.val = val;
  }

  public ByteBuf encode() {
    return Unpooled.directBuffer().writeBytes(new ChicagoObjectEncoder().encode(id, _op, colFam, key, getVal()));
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

  public void appendVal(byte[] valToAppend) {
    byte[] newVal = new byte[val.length + valToAppend.length + 1];

    System.arraycopy(val, 0, newVal, 0, val.length);
    System.arraycopy(",".getBytes(), 0, newVal, val.length, 1);
    System.arraycopy(valToAppend, 0, newVal, val.length + 1, valToAppend.length);

    val = newVal;
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
    return "id: " + id + " op: " + _op + " key: " + new String(key) + " value: " + new String(getVal());
  }
}
