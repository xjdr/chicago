package com.xjeffrose.chicago.client;

import com.xjeffrose.chicago.ChicagoMessage;
import com.xjeffrose.chicago.DefaultChicagoMessage;
import com.xjeffrose.chicago.Op;
import com.xjeffrose.xio.SSL.XioSecurityHandlerImpl;
import com.xjeffrose.xio.client.retry.BoundedExponentialBackoffRetry;
import com.xjeffrose.xio.client.retry.RetryLoop;
import com.xjeffrose.xio.client.retry.TracerDriver;
import com.xjeffrose.xio.core.XioIdleDisconnectHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.log4j.Logger;


public class ChicagoClient {
  private static final Logger log = Logger.getLogger(ChicagoClient.class);
  private final InetSocketAddress server;

  ChicagoClient(InetSocketAddress server) {

    this.server = server;
  }

  public byte[] read(byte[] key) {
    Listener<byte[]> listener = new ChicagoListener();

    connect(Op.READ, key, null, listener);

    return listener.getResponse();
  }

  public boolean write(byte[] key, byte[] value) {
    Listener<byte[]> listener = new ChicagoListener();

    connect(Op.WRITE, key, value, listener);

    return listener.getStatus();
  }

  public boolean delete(byte[] key) {
    Listener<byte[]> listener = new ChicagoListener();

    connect(Op.DELETE, key, null, listener);

    return listener.getStatus();
  }


  private void connect(Op op, byte[] key, byte[] val, Listener listener) {
    // Start the connection attempt.
    Bootstrap bootstrap = new Bootstrap();
    bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 500)
        .option(ChannelOption.SO_REUSEADDR, true)
        .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
        .option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32 * 1024)
        .option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024)
        .option(ChannelOption.TCP_NODELAY, true);
    bootstrap.group(new NioEventLoopGroup(2))
        .channel(NioSocketChannel.class)
        .handler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel channel) throws Exception {
            ChannelPipeline cp = channel.pipeline();
//            cp.addLast(new XioSecurityHandlerImpl(true).getEncryptionHandler());
            cp.addLast(new XioIdleDisconnectHandler(60, 60, 60));
            cp.addLast(new ChicagoClientCodec());
            cp.addLast(new ChicagoClientHandler(listener));
          }
        });

    BoundedExponentialBackoffRetry retry = new BoundedExponentialBackoffRetry(50, 500, 4);

    TracerDriver tracerDriver = new TracerDriver() {

      @Override
      public void addTrace(String name, long time, TimeUnit unit) {
      }

      @Override
      public void addCount(String name, int increment) {
      }
    };

    RetryLoop retryLoop = new RetryLoop(retry, new AtomicReference<>(tracerDriver));
    connect2(op, key, val, bootstrap, retryLoop);

  }

  private void connect2(Op op, byte[] key, byte[] val, Bootstrap bootstrap, RetryLoop retryLoop) {
    ChannelFutureListener listener = new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) {
        if (!future.isSuccess()) {
          try {
            retryLoop.takeException((Exception) future.cause());
            log.error("==== Service connect failure (will retry)", future.cause());
            connect2(op, key, val, bootstrap, retryLoop);
          } catch (Exception e) {
            log.error("==== Service connect failure ", future.cause());
            // Close the connection if the connection attempt has failed.
          }
        } else {
          log.debug("Chicago connected: ");
          Channel ch = future.channel();

          ChicagoMessage chicagoMessage = new DefaultChicagoMessage(op, key, val);

          try {
            ch.writeAndFlush(chicagoMessage);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
    };
    bootstrap.connect(server).addListener(listener);
  }
}
