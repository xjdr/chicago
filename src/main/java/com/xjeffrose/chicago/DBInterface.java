package com.xjeffrose.chicago;

public interface DBInterface {

  boolean write(byte[] colFam, byte[] key, byte[] val);

  byte[] read(byte[] colFam, byte[] key);

  boolean delete(byte[] colFam, byte[] key);

  byte[] tsWrite(byte[] colFam, byte[] val);

  byte[] batchWrite(byte[] colFam, byte[] val);

  byte[] stream(byte[] colFam, byte[] key);
}