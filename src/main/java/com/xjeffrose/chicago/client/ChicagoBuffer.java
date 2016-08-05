package com.xjeffrose.chicago.client;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.xjeffrose.chicago.ChiUtil;
import com.xjeffrose.chicago.DefaultChicagoMessage;
import com.xjeffrose.chicago.Op;
import io.netty.channel.ChannelFuture;
import io.netty.util.internal.PlatformDependent;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class ChicagoBuffer {
  private final ConnectionPoolManager connectionPoolMgr;
  private final BaseChicagoClient chicagoClient;
  private final ScheduledExecutorService flushExecutor = Executors
      .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
          .setNameFormat("chicago-connection-check")
          .build());
  private final Map<String, Deque<ColFamBufferRequest>> chicagoMessageMap = PlatformDependent.newConcurrentHashMap();

  public ChicagoBuffer(ConnectionPoolManager connectionPoolMgr, BaseChicagoClient chicagoClient) {
    this.connectionPoolMgr = connectionPoolMgr;
    this.chicagoClient = chicagoClient;

    flushExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          flush();
        } catch (ChicagoClientTimeoutException e) {
          e.printStackTrace();
        } catch (ChicagoClientException e) {
          e.printStackTrace();
        }
      }
    }, 0, 10, TimeUnit.MILLISECONDS);
  }

  public ListenableFuture<List<byte[]>> append(byte[] colFam, byte[] value) {
    synchronized (chicagoMessageMap) {
      ColFamBufferRequest bufferRequest = getOrCreate(colFam);
      //System.out.println("Writing to"  + bufferRequest.toString());
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

  public void flush() throws ChicagoClientTimeoutException, ChicagoClientException {
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
          byte[] value = colFamBufferRequest.getConsolidatedValue();

          //Create a Chicago Message appending all the values together.
          //DefaultChicagoMessage chicagoMessage = new DefaultChicagoMessage(id, Op.TS_WRITE,colFam,key,value);
          //System.out.println("Wrtitng colFam=" + new String(colFam));
          for (String node : colFamBufferRequest.getNodes()) {
            if (node == null) {
            } else {
              ChannelFuture cf = null;
              try {
                cf = connectionPoolMgr.getNode(node);
              } catch (ChicagoClientTimeoutException e) {
                e.printStackTrace();
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
              if (cf.channel().isWritable()) {
                UUID id = UUID.randomUUID();
                connectionPoolMgr.addToFutureMap(id, colFamBufferRequest.getFuture(node));
                cf.channel()
                    .writeAndFlush(new DefaultChicagoMessage(id, Op.TS_WRITE, colFam, null, value));
                connectionPoolMgr.releaseChannel(node, cf);
              }
            }
          }
        });
  }
}
