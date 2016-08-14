package com.xjeffrose.chicago.client;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.net.InetSocketAddress;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RequestMuxer<T> {
  private static final int POOL_SIZE = 4;

  private final String addr;
  @Setter
  private ChicagoConnector connector;
  private final EventLoopGroup workerLoop;
  private final AtomicBoolean isRunning = new AtomicBoolean();
  private final LinkedBlockingDeque<ChannelFuture> connectionQ = new LinkedBlockingDeque<>();
  @Getter
  private final LinkedBlockingDeque<MuxedMessage<T>> messageQ = new LinkedBlockingDeque<MuxedMessage<T>>();

  public RequestMuxer(String addr, ChannelHandler handler, EventLoopGroup workerLoop) {
    this.addr = addr;
    this.workerLoop = workerLoop;
    this.connector = new ChicagoConnector(handler, workerLoop);

  }

  public void start() throws Exception {
    buildInitialConnectionQ();
    blockAndAwaitPool();
    isRunning.set(true);

    workerLoop.scheduleAtFixedRate(() -> {
      if (messageQ.size() > 0) {
        drainMessageQ();
      }
    }, 0 ,200, TimeUnit.MILLISECONDS);
  }

  public void shutdownGracefully() {
    isRunning.set(false);
  }

  private InetSocketAddress address(String node) {
    String chunks[] = node.split(":");
    return new InetSocketAddress(chunks[0], Integer.parseInt(chunks[1]));
  }

  private void buildInitialConnectionQ() {
    for (int i = 0; i < POOL_SIZE; i++) {
      Futures.addCallback(connector.connect(address(addr)), new FutureCallback<ChannelFuture>() {
        @Override
        public void onSuccess(@Nullable ChannelFuture channelFuture) {
          connectionQ.addLast(channelFuture);
        }

        @Override
        public void onFailure(Throwable throwable) {
          log.error("Error connecting to " + addr, throwable);
        }
      });
    }
  }

  void rebuildConnectionQ() {
    rebuildConnectionQ(this.connectionQ);
  }

  private void rebuildConnectionQ(LinkedBlockingDeque<ChannelFuture> connectionQ) {
    connectionQ.stream().forEach(xs -> {
      ChannelFuture cf = connectionQ.pollFirst();
      if (cf.channel().isWritable()) {
        try {
          connectionQ.putLast(cf);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      } else {
        connector.connect(address(addr));
      }
    });
  }

  private boolean blockAndAwaitPool(long timeout, TimeUnit timeUnit) {

    return blockAndAwaitPool();
  }

  private boolean blockAndAwaitPool() {
    while (connectionQ.size() != POOL_SIZE) {
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        e.printStackTrace();
        return false;
      }
    }

    return true;
  }

  public void write(T sendReq, SettableFuture<Boolean> f) {
    if (isRunning.get()) {
      final MuxedMessage<T> mm = new MuxedMessage<>(sendReq, f);
      messageQ.addLast(mm);
    }
  }

  private Channel requestNode() {
    ChannelFuture cf = null;
    try {
      cf = connectionQ.poll(250, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      rebuildConnectionQ(connectionQ);
      blockAndAwaitPool();
      try {
        cf = connectionQ.poll(250, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e1) {
        log.error("Could not establish viable connection to " + addr, e1);
      }
    }

    if ((cf != null) && cf.isSuccess()) {
      if (cf.channel().isWritable()) {
        return cf.channel();
      } else {
        connectionQ.remove(cf);
        rebuildConnectionQ(connectionQ);
        return requestNode();
      }
    } else {
      connectionQ.remove(cf);
      rebuildConnectionQ(connectionQ);
      return requestNode();
    }
  }

  private void drainMessageQ() {
    if (isRunning.get()) {
      while(messageQ.peek() != null) {
        final MuxedMessage<T> mm = messageQ.poll();
        requestNode().writeAndFlush(mm.getMsg()).addListener(new GenericFutureListener<Future<? super Void>>() {
          @Override
          public void operationComplete(Future<? super Void> future) throws Exception {
            if (future.isSuccess()) {
              mm.getF().set(true);
            } else {
              mm.getF().set(false);
              mm.getF().setException(future.cause());
            }
          }
        });
      }
    }
  }

  @Data
  private class MuxedMessage<T> {
    private final T msg;
    private final SettableFuture<Boolean> f;
  }

}
