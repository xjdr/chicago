package com.xjeffrose.chicago;

public enum Op {
  READ(0),
  WRITE(1),
  DELETE(2),
  RESPONSE(3),
  TS_WRITE(4),
  STREAM(5),
  CHILD_NODE_ADDED(6),
  CHILD_NODE_MODIFIED(7),
  CHILD_NODE_REMOVED(8),
  FILE_MODIFIED(9),
  LOCK_ACQUIRED(10),
  LOCK_CONFLICT(11),
  MASTER_FAILED(12),
  MASTER_BALLOT(13),
  INVALIDATE_CACHE(14),
  MARK_NODE_DIRTY(15),
  REPLICATION_EVENT(16);

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
        return STREAM;
      case 6:
        return CHILD_NODE_ADDED;
      case 7:
        return CHILD_NODE_MODIFIED;
      case 8:
        return CHILD_NODE_REMOVED;
      case 9:
        return FILE_MODIFIED;
      case 10:
        return LOCK_ACQUIRED;
      case 11:
        return LOCK_CONFLICT;
      case 12:
        return MASTER_FAILED;
      case 13:
        return MASTER_BALLOT;
      case 14:
        return INVALIDATE_CACHE;
      case 15:
        return MARK_NODE_DIRTY;
      case 16:
        return REPLICATION_EVENT;
    }
    return null;
  }
}
