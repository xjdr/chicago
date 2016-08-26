package com.xjeffrose.chicago;

public enum Op {
  READ(0),
  WRITE(1),
  DELETE(2),
  RESPONSE(3),
  TS_WRITE(4),
  BATCH_WRITE(5),
  STREAM(6),
  GET_OFFSET(7),
  STREAM_RESPONSE(8),
  SCAN_KEYS(9),
  PAXOS_PROPOSE(10),
  PAXOS_PROMISE(11),
  PAXOS_ACCEPT(12),
  MPAXOS_PROPOSE_WRITE(13),
  MPAXOS_PROPOSE_TS_WRITE(14);


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
      case 4:
        return TS_WRITE;
      case 5:
        return BATCH_WRITE;
      case 6:
        return STREAM;
      case 7:
        return GET_OFFSET;
      case 8:
        return STREAM_RESPONSE;
      case 9:
        return SCAN_KEYS;
      case 10:
        return PAXOS_PROPOSE;
      case 11:
        return PAXOS_PROMISE;
      case 12:
        return PAXOS_ACCEPT;
      case 13:
        return MPAXOS_PROPOSE_WRITE;
      case 14:
        return MPAXOS_PROPOSE_TS_WRITE;
    }
    return null;
  }
}
