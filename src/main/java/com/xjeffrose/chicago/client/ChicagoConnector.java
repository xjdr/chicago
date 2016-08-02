package com.xjeffrose.chicago.client;

import com.google.common.util.concurrent.SettableFuture;
import com.xjeffrose.xio.SSL.XioSecurityHandlerImpl;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.SimpleChannelPool;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.internal.PlatformDependent;
import java.net.InetSocketAddress;
import java.util.Deque;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChicagoConnector {
  private static final Logger log = LoggerFactory.getLogger(ChicagoConnector.class);
  private final EventLoopGroup workerLoop;
  private final InetSocketAddress server;
  private final Map<UUID, SettableFuture<byte[]>> futureMap;
  private final Bootstrap bootstrap;

  private final Deque<Channel> deque = PlatformDependent.newConcurrentDeque();
  private final SimpleChannelPool channelPool;

  public ChicagoConnector(EventLoopGroup workerLoop, InetSocketAddress server, Map<UUID, SettableFuture<byte[]>> futureMap) {

    this.workerLoop = workerLoop;
    this.server = server;
    this.futureMap = futureMap;
    this.bootstrap = getBootstrap(server, futureMap);
    this.channelPool = buildChannelPool(getBootstrap(server, futureMap));
  }

  private SimpleChannelPool buildChannelPool(Bootstrap b) {
    return new SimpleChannelPool(b.remoteAddress(server), new ChannelPoolHandler() {
      @Override
      public void channelReleased(Channel channel) throws Exception {

      }

      @Override
      public void channelAcquired(Channel channel) throws Exception {

      }

      @Override
      public void channelCreated(Channel channel) throws Exception {

      }
    });
  }

  private Bootstrap getBootstrap(InetSocketAddress server, Map<UUID, SettableFuture<byte[]>> futureMap) {
    // Start the connection attempt.
    Bootstrap bootstrap = new Bootstrap();
    bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 500)
        .option(ChannelOption.SO_REUSEADDR, true)
        .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
        .option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32 * 1024)
        .option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024)
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_KEEPALIVE, true);
    bootstrap.group(workerLoop)
        .channel(NioSocketChannel.class)
        .handler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel channel) throws Exception {
            ChannelPipeline cp = channel.pipeline();
            cp.addLast(new XioSecurityHandlerImpl(true).getEncryptionHandler());
            cp.addLast(new ChicagoClientCodec());
            cp.addLast(new ChicagoClientHandler(futureMap));
          }
        });

    return bootstrap;
  }

  public Future<Channel> acquire() {
    return channelPool.acquire();
  }

  public void release(Channel ch) {
    channelPool.release(ch);
  }

}



