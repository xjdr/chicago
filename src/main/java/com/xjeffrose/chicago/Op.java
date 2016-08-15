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
  SCAN(9),
  SCAN_KEYS(10);


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
        return SCAN;
      case 10:
        return SCAN_KEYS;
//      case 8:
//        return CHILD_NODE_REMOVED;
//      case 9:
//        return FILE_MODIFIED;
//      case 10:
//        return LOCK_ACQUIRED;
//      case 11:
//        return LOCK_CONFLICT;
//      case 12:
//        return MASTER_FAILED;
//      case 13:
//        return MASTER_BALLOT;
//      case 14:
//        return INVALIDATE_CACHE;
//      case 15:
//        return MARK_NODE_DIRTY;
//      case 16:
//        return REPLICATION_EVENT;
    }
    return null;
  }
}
