package com.xjeffrose.chicago;

public interface ChicagoMessage extends ChicagoObject {

  Op getOp();

  byte[] getKey();

  byte[] getVal();

  boolean getSuccess();

  byte[] getColFam();
}
