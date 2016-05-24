package com.xjeffrose.chicago.client;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ChicagoStream {
  private final ConcurrentLinkedDeque<UUID> idList = new ConcurrentLinkedDeque<>();
  private Listener listener;


  public ChicagoStream(Listener listener) {

    this.listener = listener;
  }

  public ListenableFuture<byte[]> getStream() {
    final ExecutorService exe = Executors.newFixedThreadPool(4);


  ListeningExecutorService executor = MoreExecutors.listeningDecorator(exe);
    ListenableFuture<byte[]> responseFuture = executor.submit(new Callable<byte[]>() {
      @Override
      public byte[] call() throws Exception {
        final long startTime = System.currentTimeMillis();
        try {
          byte[] resp =  (byte[]) listener.getResponse(idList.getFirst());
          if (resp != null) {
            return resp;
          }
        } catch (ChicagoClientTimeoutException e) {
          e.printStackTrace();
        }
        return null;
      }
    });

    return responseFuture;
  }

  public void addID(UUID id) {
    idList.add(id);
  }
}
