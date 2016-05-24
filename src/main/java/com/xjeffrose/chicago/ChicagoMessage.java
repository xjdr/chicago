package com.xjeffrose.chicago;

import io.netty.buffer.ByteBuf;
import java.util.UUID;

public interface ChicagoMessage extends ChicagoObject {

  UUID getId();

  Op getOp();

  byte[] getKey();

  byte[] getVal();

  boolean getSuccess();

  byte[] getColFam();

  ByteBuf getStream();
}
