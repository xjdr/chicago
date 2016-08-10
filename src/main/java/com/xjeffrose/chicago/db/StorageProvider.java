package com.xjeffrose.chicago.db;

import com.xjeffrose.chicago.ZkClient;
import java.util.List;

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

  void setZkClient(ZkClient zkClient);

  List<byte[]> getKeys(byte[] colFam, byte[] offset);

  List<String> getColFams();

}
