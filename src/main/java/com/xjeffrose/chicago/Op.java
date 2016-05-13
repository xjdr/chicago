package com.xjeffrose.chicago;

public enum Op {
  READ(0),
  WRITE(1),
  DELETE(2),
  RESPONSE(3);

  private int i;

  Op(int i) {

    this.i = i;
  }

  public int getOp() {
    return i;
  }

  public static Op fromInt(int x) {
    switch(x) {
      case 0:
        return READ;
      case 1:
        return WRITE;
      case 2:
        return DELETE;
      case 3:
        return RESPONSE;
    }
    return null;
  }
}
