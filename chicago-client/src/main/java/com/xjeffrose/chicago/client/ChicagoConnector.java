package com.xjeffrose.chicago.client;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.xjeffrose.chicago.ChicagoCodec;
import com.xjeffrose.chicago.ChicagoMessageAggregator;
import com.xjeffrose.xio.SSL.XioSecurityHandlerImpl;
import com.xjeffrose.xio.client.retry.BoundedExponentialBackoffRetry;
import com.xjeffrose.xio.client.retry.RetryLoop;
import com.xjeffrose.xio.client.retry.TracerDriver;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class ChicagoConnector {
  private final EventLoopGroup workerLoop;
  private final ChannelHandler handler;
  private final Bootstrap bootstrap;

  public ChicagoConnector(ChannelHandler handler) {
    this.handler = handler;
    this.workerLoop = new NioEventLoopGroup(5,
        new ThreadFactoryBuilder()
            .setNameFormat("chicagoClient-nioEventLoopGroup-%d")
            .build()
    );
    this.bootstrap = buildBootstrap();
  }

  public ChicagoConnector(ChannelHandler handler, EventLoopGroup workerLoop) {
    this.handler = handler;
    this.workerLoop = workerLoop;
    this.bootstrap = buildBootstrap();
  }

  public ListenableFuture<ChannelFuture> connect(InetSocketAddress addr) {
    return connect(addr, bootstrap.clone(), buildRetryLoop());
  }

  private Bootstrap buildBootstrap() {
    // Start the connection attempt.
    return new Bootstrap()
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 500)
        .option(ChannelOption.SO_REUSEADDR, true)
        .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
        .option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32 * 1024)
        .option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024)
        .option(ChannelOption.TCP_NODELAY, true)
        .group(workerLoop)
        .channel(NioSocketChannel.class)
        .handler(new ChannelInitializer<NioSocketChannel>() {
          @Override
          protected void initChannel(NioSocketChannel channel) throws Exception {
            ChannelPipeline cp = channel.pipeline();
            cp.addLast(new XioSecurityHandlerImpl(true).getEncryptionHandler());
            //cp.addLast(new XioIdleDisconnectHandler(20, 20, 20));
            cp.addLast(new ChicagoCodec());
            cp.addLast(new ChicagoMessageAggregator());
            cp.addLast(handler);
          }
        });
  }

  private RetryLoop buildRetryLoop() {
    BoundedExponentialBackoffRetry retry = new BoundedExponentialBackoffRetry(50, 500, 4);

    TracerDriver tracerDriver = new TracerDriver() {

      @Override
      public void addTrace(String name, long time, TimeUnit unit) {
      }

      @Override
      public void addCount(String name, int increment) {
      }
    };

    return new RetryLoop(retry, new AtomicReference<>(tracerDriver));

  }

  private SettableFuture<ChannelFuture> connect(InetSocketAddress server, Bootstrap bootstrap, RetryLoop retryLoop) {
    final SettableFuture<ChannelFuture> f = SettableFuture.create();
    ChannelFutureListener listener = new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) {
        if (!future.isSuccess()) {
          try {
            retryLoop.takeException((Exception) future.cause());
            log.info("==== Service connect failure (will retry)", future.cause());
            connect(server, bootstrap, retryLoop);
          } catch (Exception e) {
            log.error("==== Service connect failure ", future.cause());
            // Close the connection if the connection attempt has failed.
            future.channel().close();
            f.setException((Exception) e);
          }
        } else {
          log.debug("Chicago connected to: " + server);
          String hostname = server.getAddress().getHostAddress();
          if (hostname.equals("localhost")) {
            hostname = "127.0.0.1";
          }
          log.debug("Adding hostname: " + hostname + ":" + ((InetSocketAddress) future.channel().remoteAddress()).getPort());
          f.set(future);
        }
      }
    };

    bootstrap.connect(server).addListener(listener);

    return f;
  }
}
