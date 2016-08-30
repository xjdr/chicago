package com.xjeffrose.chicago.client;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.xjeffrose.chicago.ChicagoMessage;
import com.xjeffrose.chicago.NodeListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.EventLoopGroup;
import io.netty.util.internal.PlatformDependent;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ConnectionPoolManagerImpl implements ConnectionPoolManager, NodeListener {
  private final ChannelHandler handler;
  private final EventLoopGroup workerLoop;
  private final Map<String, RequestMuxer<ChicagoMessage>> connectionMap = PlatformDependent.newConcurrentHashMap();
  private List<String> nodeList;

  public ConnectionPoolManagerImpl(List<String> nodeList, ChannelHandler handler, EventLoopGroup workerLoop) {
    this.nodeList = nodeList;
    this.handler = handler;
    this.workerLoop = workerLoop;
  }

  public void start() {
    buildConnectionMap(nodeList, handler, workerLoop);
    blockAndAwaitPool();
  }

  public void stop() {
    connectionMap.forEach((k,v) ->{
      v.close();
    });
  }

  @Override
  public void checkConnection() {
    //TODO(JR): No idea what this was supposed to do. Prob check a connection?!?
  }

  private void buildConnectionMap(List<String> nodeList, ChannelHandler handler, EventLoopGroup workerLoop) {
    nodeList.stream().forEach(xs -> {
      RequestMuxer<ChicagoMessage> mux = new RequestMuxer<>(xs, handler, workerLoop);
      try {
        mux.start();
      } catch (Exception e) {
        //TODO(JR): Determine best course for recovery here
        e.printStackTrace();
      }
      connectionMap.put(xs, mux);
    });
  }

  private boolean blockAndAwaitPool(long timeout, TimeUnit timeUnit) {

    return blockAndAwaitPool();
  }

  private boolean blockAndAwaitPool() {
    while (connectionMap.size() != nodeList.size()) {
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        e.printStackTrace();
        return false;
      }
    }

    return true;
  }

  @Override
  public ListenableFuture<Boolean> write(String addr, ChicagoMessage msg) {
    if (connectionMap.containsKey(addr)) {
      SettableFuture<Boolean> f = SettableFuture.create();
      connectionMap.get(addr).write(msg, f);
      return f;
    } else {
      rebuildConnectionMap(addr, connectionMap);
      blockAndAwaitPool();
      return write(addr, msg);
    }
  }

  private void rebuildConnectionMap(String addr, Map<String, RequestMuxer<ChicagoMessage>> connectionMap) {
    connectionMap.get(addr).rebuildConnectionQ();
  }

  @Override
  public void nodeAdded(Object node) {
    RequestMuxer<ChicagoMessage> mux = new RequestMuxer<>((String)node, handler, workerLoop);
    try {
      mux.start();
    } catch (Exception e) {
      //TODO(JR): Determine best course for recovery here
      e.printStackTrace();
    }
    connectionMap.put((String)node, mux);
  }

  @Override
  public void nodeRemoved(Object node) {
    RequestMuxer mux = connectionMap.remove((String)node);
    mux.close();
  }

}
