package com.xjeffrose.chicago.db;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.util.concurrent.ExecutionException;

public class DBManagerUnitTest {

  StorageProvider backend;
  DBManager manager;

  @Before
  public void setUp() {
    backend = Mockito.mock(StorageProvider.class);
    manager = new DBManager(backend);
    manager.startAsync().awaitRunning();
  }

  @After
  public void tearDown() {
    manager.stopAsync().awaitTerminated();
  }

  @Test
  public void testWrite() throws InterruptedException, ExecutionException {
    byte[] colFam = "colFam".getBytes();
    byte[] key = "key".getBytes();
    byte[] val = "val".getBytes();

    manager.write(colFam, key, val).get();

    Mockito.verify(backend).write(Matchers.eq(colFam), Matchers.eq(key), Matchers.eq(val));
  }

  @Test
  public void testRead() throws InterruptedException, ExecutionException {
    byte[] colFam = "colFam".getBytes();
    byte[] key = "key".getBytes();

    manager.read(colFam, key).get();

    Mockito.verify(backend).read(Matchers.eq(colFam), Matchers.eq(key));
  }

  @Test
  public void testDelete() throws InterruptedException, ExecutionException {
    byte[] colFam = "colFam".getBytes();
    byte[] key = "key".getBytes();

    manager.delete(colFam, key).get();

    Mockito.verify(backend).delete(Matchers.eq(colFam), Matchers.eq(key));
  }

  @Test
  public void testDeleteNullKey() throws InterruptedException, ExecutionException {
    byte[] colFam = "colFam".getBytes();
    byte[] key = null;

    manager.delete(colFam, key).get();

    Mockito.verify(backend).delete(Matchers.eq(colFam));
  }

  @Test
  public void testTsWriteNullKey() throws InterruptedException, ExecutionException {
    byte[] colFam = "colFam".getBytes();
    byte[] key = null;
    byte[] val = "val".getBytes();

    manager.tsWrite(colFam, key, val).get();

    Mockito.verify(backend).tsWrite(Matchers.eq(colFam), Matchers.eq(val));
  }

  @Test
  public void testBatchWrite() throws InterruptedException, ExecutionException {
    byte[] colFam = "colFam".getBytes();
    byte[] val = "val".getBytes();

    manager.batchWrite(colFam, val).get();

    Mockito.verify(backend).batchWrite(Matchers.eq(colFam), Matchers.eq(val));
  }

  @Test
  public void testStream() throws InterruptedException, ExecutionException {
    byte[] colFam = "colFam".getBytes();
    byte[] key = "key".getBytes();

    manager.stream(colFam, key).get();

    Mockito.verify(backend).stream(Matchers.eq(colFam), Matchers.eq(key));
  }

  @Test
  public void testTsWrite() throws InterruptedException, ExecutionException {
    byte[] colFam = "colFam".getBytes();
    byte[] key = "key".getBytes();
    byte[] val = "val".getBytes();

    manager.tsWrite(colFam, key, val).get();

    Mockito.verify(backend).tsWrite(Matchers.eq(colFam), Matchers.eq(key), Matchers.eq(val));
  }

}
