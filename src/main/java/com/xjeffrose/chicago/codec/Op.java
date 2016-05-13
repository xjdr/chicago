package com.xjeffrose.chicago.codec;

public enum Op {
  READ(1),
  WRITE(2),
  DELETE(3);

  private int i;

  Op(int i) {

    this.i = i;
  }

  public int getOp() {
    return i;
  }
}
