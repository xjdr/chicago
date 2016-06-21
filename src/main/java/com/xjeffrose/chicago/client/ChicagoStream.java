package com.xjeffrose.chicago.client;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChicagoStream implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(ChicagoStream.class.getName());
  private static final long TIMEOUT = 1000;
  private static final boolean TIMEOUT_ENABLED = true;

  private final ConcurrentLinkedDeque<UUID> idList = new ConcurrentLinkedDeque<>();
  private Listener listener;
  private final ExecutorService exe = Executors.newFixedThreadPool(4);

  public ChicagoStream(Listener listener) {

    this.listener = listener;
  }

  public ListenableFuture<byte[]> getStream() {

  ListeningExecutorService executor = MoreExecutors.listeningDecorator(exe);
    ListenableFuture<byte[]> responseFuture = executor.submit(new Callable<byte[]>() {
      @Override
      public byte[] call() throws Exception {
        final long startTime = System.currentTimeMillis();
        while (idList.isEmpty()) {
          if (TIMEOUT_ENABLED && (System.currentTimeMillis() - startTime) > TIMEOUT) {
            Thread.currentThread().interrupt();
            throw new ChicagoClientTimeoutException();
          }
          try {
            Thread.sleep(1);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
        try {
          byte[] resp =  (byte[]) listener.getResponse(idList);
          if (resp != null) {
            return resp;
          }
        } catch (ChicagoClientTimeoutException e) {
          e.printStackTrace();
        }
        return call();
      }
    });

    return responseFuture;
  }

  public void addID(UUID id) {
    idList.add(id);
  }

  @Override
  public void close() throws Exception {
    exe.shutdown();
    idList.stream().forEach(xs -> {
	listener.removeID(xs);
    });
  }
}
