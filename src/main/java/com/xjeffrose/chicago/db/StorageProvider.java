package com.xjeffrose.chicago.db;

public interface StorageProvider extends AutoCloseable {

  boolean write(byte[] colFam, byte[] key, byte[] val);

  byte[] read(byte[] colFam, byte[] key);

  boolean delete(byte[] colFam, byte[] key);

  boolean delete(byte[] colFam);

  byte[] tsWrite(byte[] colFam, byte[] val);

  byte[] batchWrite(byte[] colFam, byte[] val);

  byte[] stream(byte[] colFam, byte[] key);

  byte[] tsWrite(byte[] colFam, byte[] key, byte[] val);

  void close();

  void open();
}