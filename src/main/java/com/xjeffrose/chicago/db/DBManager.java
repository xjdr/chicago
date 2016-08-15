package com.xjeffrose.chicago.db;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class DBManager extends AbstractExecutionThreadService {
  abstract class Message {
    abstract void process();
    abstract void fail(Throwable t);
  }

  class StopMessage extends Message {
    @Override
    void process() {
      // stop accepting new messages
      running.set(false);
    }

    @Override
    void fail(Throwable t) {
    }
  }

  class WaitMessage extends Message {
    final SettableFuture<Boolean> promise;

    WaitMessage(SettableFuture<Boolean> promise) {
      this.promise = promise;
    }

    @Override
    void process() {
      if (queue.size() == 0) {
        log.debug("queue is empty!");
        promise.set(true);
      } else {
        log.error("queue size wasn't 0, waiting again");
        post(new WaitMessage(promise));
      }
    }

    @Override
    void fail(Throwable t) {
      promise.setException(t);
    }
  }

  class ReadMessage extends Message {
    final byte[] colFam;
    final byte[] key;
    final SettableFuture<byte[]> promise;

    ReadMessage(byte[] colFam, byte[] key, SettableFuture<byte[]> promise) {
      this.colFam = colFam;
      this.key = key;
      this.promise = promise;
    }

    @Override
    void process() {
      promise.set(backend.read(colFam, key));
    }

    @Override
    void fail(Throwable t) {
      promise.setException(t);
    }
  }

  class WriteMessage extends Message {
    final byte[] colFam;
    final byte[] key;
    final byte[] value;
    final SettableFuture<Boolean> promise;

    WriteMessage(byte[] colFam, byte[] key, byte[] value, SettableFuture<Boolean> promise) {
      this.colFam = colFam;
      this.key = key;
      this.value = value;
      this.promise = promise;
    }

    @Override
    void process() {
      promise.set(backend.write(colFam, key, value));
    }

    @Override
    void fail(Throwable t) {
      promise.setException(t);
    }
  }

  class BatchWriteMessage extends Message {
    final byte[] colFam;
    final byte[] value;
    final SettableFuture<byte[]> promise;

    BatchWriteMessage(byte[] colFam, byte[] value, SettableFuture<byte[]> promise) {
      this.colFam = colFam;
      this.value = value;
      this.promise = promise;
    }

    @Override
    void process() {
      promise.set(backend.batchWrite(colFam, value));
    }

    @Override
    void fail(Throwable t) {
      promise.setException(t);
    }
  }

  class TimeSeriesWriteMessage extends Message {
    final byte[] colFam;
    final byte[] key;
    final byte[] value;
    final SettableFuture<byte[]> promise;

    TimeSeriesWriteMessage(byte[] colFam, byte[] key, byte[] value, SettableFuture<byte[]> promise) {
      this.colFam = colFam;
      this.key = key;
      this.value = value;
      this.promise = promise;
    }

    @Override
    void process() {
      if (key == null) {
        promise.set(backend.tsWrite(colFam, value));
      } else {
        promise.set(backend.tsWrite(colFam, key, value));
      }
    }

    @Override
    void fail(Throwable t) {
      promise.setException(t);
    }
  }

  class DeleteMessage extends Message {
    final byte[] colFam;
    final byte[] key;
    final SettableFuture<Boolean> promise;

    DeleteMessage(byte[] colFam, byte[] key, SettableFuture<Boolean> promise) {
      this.colFam = colFam;
      this.key = key;
      this.promise = promise;
    }

    @Override
    void process() {
      if (key == null) {
        promise.set(backend.delete(colFam));
      } else {
        promise.set(backend.delete(colFam, key));
      }
    }

    @Override
    void fail(Throwable t) {
      promise.setException(t);
    }
  }

  class StreamingReadMessage extends Message {
    final byte[] colFam;
    final byte[] key;
    final SettableFuture<List<DBRecord>> promise;

    StreamingReadMessage(byte[] colFam, byte[] key, SettableFuture<List<DBRecord>> promise) {
      this.colFam = colFam;
      this.key = key;
      this.promise = promise;
    }

    @Override
    void process() {
      promise.set(backend.stream(colFam, key));
    }

    @Override
    void fail(Throwable t) {
      promise.setException(t);
    }
  }


  class ScanColFamilyMessage extends Message {
    final SettableFuture<List<String>> promise;

    ScanColFamilyMessage(SettableFuture<List<String>> promise) {
      this.promise = promise;
    }

    @Override
    void process() {
      promise.set(backend.getColFams());
    }

    @Override
    void fail(Throwable t) {
      promise.setException(t);
    }
  }

  class ScanKeyMessage extends Message {
    final byte[] colFam;
    final SettableFuture<List<byte[]>> promise;

    ScanKeyMessage(byte[] colFam, SettableFuture<List<byte[]>> promise) {
      this.colFam = colFam;
      this.promise = promise;
    }

    @Override
    void process() {
      promise.set(backend.getKeys(colFam, new byte[0]));
    }

    @Override
    void fail(Throwable t) {
      promise.setException(t);
    }
  }

  private final AtomicBoolean running = new AtomicBoolean(false);
  private final BlockingQueue<Message> queue = new LinkedBlockingQueue<Message>();
  private final StorageProvider backend;

  public DBManager(StorageProvider backend) {
    this.backend = backend;
  }

  private void openDatabase() {
    backend.open();
  }

  private void closeDatabase() {
    backend.close();
  }

  private void post(Message message) {
    try {
      log.debug("posting message: {}", message);
      queue.put(message); // use offer with timeout?
    } catch (InterruptedException e) {
      log.error("Couldn't post message ", e);
      message.fail(e);
    }
  }

  private void processMessage(Message message) {
    // ignore null messages
    if (message == null) {
      return;
    }
    try {
      message.process();
    } catch (Exception e) {
      log.error("processMessage ", e);
      message.fail(e);
    }
  }

  private void drainQueue() {
    List<Message> messages = new ArrayList<>();
    queue.drainTo(messages);
    messages.forEach((m) -> processMessage(m));
  }

  @Override // AbstractExecutionThreadService.run
  protected void run() {
    while(running.get()) {
      log.debug("running iteration");
      // get the next message from the queue
      Message message = queue.poll(); // block, don't use timeout
      // process message
      processMessage(message);
    }
  }

  @Override // AbstractExecutionThreadService.startUp
  protected void startUp() {
    log.debug("starting up");
    // open database
    openDatabase();
    // start accepting messages
    running.set(true);
  }

  @Override // AbstractExecutionThreadService.triggerShutdown
  protected void triggerShutdown() {
    log.debug("shutdown triggered");
    post(new StopMessage());
  }

  @Override // AbstractExecutionThreadService.shutDown
  protected void shutDown() {
    log.debug("shutting down");
    // drain the incoming message queue
    drainQueue();
    // close database
    closeDatabase();
  }

  @Override // AbstractExecutionThreadService.serviceName
  protected String serviceName() {
    return "DBManager[RocksDB]"; // TODO(CK): get name from impl
  }

  public ListenableFuture<byte[]> read(byte[] colFam, byte[] key) {
    SettableFuture<byte[]> promise = SettableFuture.create();
    post(new ReadMessage(colFam, key, promise));
    return promise;
  }

  public ListenableFuture<Boolean> write(byte[] colFam, byte[] key, byte[] value) {
    SettableFuture<Boolean> promise = SettableFuture.create();
    post(new WriteMessage(colFam, key, value, promise));
    return promise;
  }

  public ListenableFuture<byte[]> batchWrite(byte[] colFam, byte[] value) {
    SettableFuture<byte[]> promise = SettableFuture.create();
    post(new BatchWriteMessage(colFam, value, promise));
    return promise;
  }

  public ListenableFuture<byte[]> tsWrite(byte[] colFam, byte[] key, byte[] value) {
    SettableFuture<byte[]> promise = SettableFuture.create();
    post(new TimeSeriesWriteMessage(colFam, key, value, promise));
    return promise;
  }

  public ListenableFuture<Boolean> delete(byte[] colFam, byte[] key) {
    SettableFuture<Boolean> promise = SettableFuture.create();
    post(new DeleteMessage(colFam, key, promise));
    return promise;
  }

  public ListenableFuture<List<DBRecord>> stream(byte[] colFam, byte[] key) {
    SettableFuture<List<DBRecord>> promise = SettableFuture.create();
    post(new StreamingReadMessage(colFam, key, promise));
    return promise;
  }

  @VisibleForTesting
  public ListenableFuture<Boolean> waitForEmptyQueue() {
    SettableFuture<Boolean> promise = SettableFuture.create();
    post(new WaitMessage(promise));
    return promise;
  }

  public ListenableFuture<List<String>> getColFamilies() {
    SettableFuture<List<String>> promise = SettableFuture.create();
    post(new ScanColFamilyMessage(promise));
    return promise;
  }

  public ListenableFuture<List<byte[]>> getKeys(byte[] colFam) {
    SettableFuture<List<byte[]>> promise = SettableFuture.create();
    post(new ScanKeyMessage(colFam, promise));
    return promise;
  }
}
