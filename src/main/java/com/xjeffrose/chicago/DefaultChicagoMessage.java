package com.xjeffrose.chicago;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.DecoderResult;

public class DefaultChicagoMessage implements ChicagoMessage {
  private final Op _op;
  private final byte[] key;
  private final byte[] val;
  private DecoderResult decoderResult;

  public DefaultChicagoMessage(Op _op, byte[] key, byte[] val) {
    this._op = _op;
    this.key = key;
    this.val = val;
  }

  public ByteBuf encode() {
    byte[] op = {(byte) _op.getOp()};
    byte[] keySize = {(byte) key.length};
    byte[] valSize ={(byte) val.length};
    byte[] msgArray = new byte[op.length + keySize.length + key.length + valSize.length + val.length];

    System.arraycopy(op, 0, msgArray, 0, op.length);
    System.arraycopy(keySize, 0, msgArray, op.length, keySize.length);
    System.arraycopy(key, 0, msgArray, op.length + keySize.length, key.length);
    System.arraycopy(valSize, 0, msgArray, op.length + keySize.length + key.length , valSize.length);
    System.arraycopy(val, 0, msgArray, op.length + keySize.length + key.length + valSize.length, val.length);

    return Unpooled.directBuffer().writeBytes(msgArray);
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
}
