package com.xjeffrose.chicago.client;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.xjeffrose.chicago.ChiUtil;
import com.xjeffrose.chicago.DefaultChicagoMessage;
import com.xjeffrose.chicago.Op;
import io.netty.channel.Channel;
import io.netty.util.internal.PlatformDependent;

import java.util.*;
import java.util.concurrent.*;


public class ChicagoBuffer {
  private final ConnectionPoolManagerX connectionPoolMgr;
  private final BaseChicagoClient chicagoClient;
  private static final long TIMEOUT = 3000;
  private final ScheduledExecutorService flushExecutor = Executors
      .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
          .setNameFormat("chicago-connection-check")
          .build());
  private final Map<String, Deque<ColFamBufferRequest>> chicagoMessageMap = PlatformDependent.newConcurrentHashMap();

  public ChicagoBuffer(ConnectionPoolManagerX connectionPoolMgr, BaseChicagoClient chicagoClient) {
    this.connectionPoolMgr = connectionPoolMgr;
    this.chicagoClient = chicagoClient;

    flushExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          flush();
        } catch (ChicagoClientException e) {
          e.printStackTrace();
        }
      }
    }, 0, 10, TimeUnit.MILLISECONDS);
  }

  public ListenableFuture<List<byte[]>> append(byte[] colFam, byte[] value) {
    synchronized (chicagoMessageMap) {
      ColFamBufferRequest bufferRequest = getOrCreate(colFam);
      bufferRequest.addValue(value);
      return bufferRequest.listListenableFuture();
    }
  }

  private ColFamBufferRequest getOrCreate(byte[] colFam) {
    Deque<ColFamBufferRequest> deque = chicagoMessageMap.get(new String(colFam));
    if (deque == null) {
      Deque<ColFamBufferRequest> dequenew = PlatformDependent.newConcurrentDeque();
      chicagoMessageMap.put(new String(colFam), dequenew);
    }
    ColFamBufferRequest cfb;

    if (chicagoMessageMap.get(new String(colFam)).isEmpty()) {
      cfb = new ColFamBufferRequest(colFam, chicagoClient.getEffectiveNodes(colFam));
      chicagoMessageMap.get(new String(colFam)).addFirst(cfb);
    } else {
      if (chicagoMessageMap.get(new String(colFam)).getFirst().getSize() > ChiUtil.MaxBufferSize) {
        cfb = new ColFamBufferRequest(colFam, chicagoClient.getEffectiveNodes(colFam));
        chicagoMessageMap.get(new String(colFam)).addFirst(cfb);
      } else {
        cfb = chicagoMessageMap.get(new String(colFam)).getFirst();
      }
    }
    return cfb;
  }

  public void flush() throws ChicagoClientException {
    List<ColFamBufferRequest> colFamBufferRequests = new ArrayList<>();
    synchronized (chicagoMessageMap) {
      chicagoMessageMap.forEach((k, v) -> {
        //System.out.println("Processing " + k);
        if (v.peekLast() != null) {
          //System.out.println("Taking out  "+ v.peekLast().toString());
          colFamBufferRequests.add(v.removeLast());
        }
      });
    }

    //System.out.println("Size of colFamBufferRequests "+ colFamBufferRequests.size());
    colFamBufferRequests
        .stream()
        .forEach(colFamBufferRequest -> {
          byte[] colFam = colFamBufferRequest.getColFam();
          colFamBufferRequest.getNodes().parallelStream().forEach(node -> {
              //Create a Chicago Message appending all the values together.
              //System.out.println("Wrtitng colFam=" + new String(colFam));
              if (node == null) {
              } else {
                Channel ch = null;
                try {
                  ch = connectionPoolMgr.getNode(node).get(TIMEOUT, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                } catch (ExecutionException e) {
                  e.printStackTrace();
                } catch (TimeoutException e) {
                  e.printStackTrace();
                } catch (ChicagoClientTimeoutException e) {
                  e.printStackTrace();
                }
                if (ch.isWritable()) {
                  for(byte[] val : colFamBufferRequest.getValues()) {
                    UUID id = UUID.randomUUID();
                    connectionPoolMgr.addToFutureMap(id, colFamBufferRequest.getFuture(node));
                    ch.write(new DefaultChicagoMessage(id, Op.TS_WRITE, colFam, null, val));
                  }
                }
                ch.flush();
                connectionPoolMgr.releaseChannel(node,ch);
              }
          });
        });
  }
}
