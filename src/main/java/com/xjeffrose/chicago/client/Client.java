package com.xjeffrose.chicago.client;

import com.google.common.util.concurrent.ListenableFuture;

public interface Client extends AutoCloseable {

  void start();

  ListenableFuture<byte[]> scanKeys(byte[] colFam);

  ListenableFuture<byte[]> read(byte[] colFam, byte[] key);

  ListenableFuture<Boolean> write(byte[] colFam, byte[] key, byte[] val);

  ListenableFuture<byte[]> tsWrite(byte[] topic, byte[] val);

  ListenableFuture<byte[]> stream(byte[] topic, byte[] offset);

  ListenableFuture<Boolean> deleteColFam(byte[] colFam);

}
