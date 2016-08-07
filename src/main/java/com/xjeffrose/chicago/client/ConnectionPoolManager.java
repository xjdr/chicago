package com.xjeffrose.chicago.client;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.xjeffrose.chicago.ZkClient;
import com.xjeffrose.xio.SSL.XioSecurityHandlerImpl;
import com.xjeffrose.xio.client.retry.BoundedExponentialBackoffRetry;
import com.xjeffrose.xio.client.retry.RetryLoop;
import com.xjeffrose.xio.client.retry.TracerDriver;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.internal.PlatformDependent;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionPoolManager {
  private static final Logger log = LoggerFactory.getLogger(ConnectionPoolManager.class);
  private final static String NODE_LIST_PATH = "/chicago/node-list";
  private static final long TIMEOUT = 1000;
  protected static int MAX_RETRY = 3;
  private static boolean TIMEOUT_ENABLED = true;

  private final Map<String, Channel> connectionMap = PlatformDependent.newConcurrentHashMap();
  private final NioEventLoopGroup workerLoop = new NioEventLoopGroup(5,
      new ThreadFactoryBuilder()
          .setNameFormat("chicago-nioEventLoopGroup-%d")
          .build()
  );
  private final ZkClient zkClient;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final ScheduledExecutorService connectCheck = Executors
      .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
          .setNameFormat("chicago-connection-check")
          .build());

  private final Map<UUID, SettableFuture<byte[]>> futureMap;
  private final ChannelHandler handler;

  public ConnectionPoolManager(ZkClient zkClient, Map<UUID, SettableFuture<byte[]>> futureMap) {
    this.zkClient = zkClient;
    this.futureMap = futureMap;
    this.handler = new ChicagoClientHandler(futureMap);
  }

  public ConnectionPoolManager(String hostname, Map<UUID, SettableFuture<byte[]>> futureMap) {
    this.zkClient = null;
    connect(new InetSocketAddress(hostname.split(":")[0], Integer.parseInt(hostname.split(":")[1])));
    this.futureMap = futureMap;
    this.handler = new ChicagoClientHandler(futureMap);
  }

  public ConnectionPoolManager(List<EmbeddedChannel> hostPool, Map<UUID, SettableFuture<byte[]>> futureMap) {
    this.zkClient = null;
    this.futureMap = futureMap;
    this.handler = new ChicagoClientHandler(futureMap);

    connectionMap.put("embedded", hostPool.get(0));
  }

  public void start() {
    running.set(true);
    refreshPool();
  }

  public void addToFutureMap(UUID id, SettableFuture<byte[]> f) {
    futureMap.put(id, f);
  }

  public void stop() {
    log.info("ConnectionPoolManager stopping");
    running.set(false);
    ChannelGroup channelGroup = new DefaultChannelGroup(workerLoop.next());
    for (Channel ch : connectionMap.values()) {
      channelGroup.add(ch);
    }
    log.info("Closing channels");
    channelGroup.close().awaitUninterruptibly();
    log.info("Stopping workerLoop");
    workerLoop.shutdownGracefully();
    connectCheck.shutdownNow();
  }

  public EventLoopGroup getWorkerGroup() {
    return workerLoop;
  }

  public synchronized void checkConnection() {
    List<String> reconnectNodes = new ArrayList<>();
    buildNodeList().forEach(s -> {
      if (connectionMap.get(s) == null) {
        log.debug("Channel not present for " + s);
        reconnectNodes.add(s);
      } else if (connectionMap.get(s) != null && !connectionMap.get(s).isWritable()) {
        log.debug("Channel not writeable for " + s);
        reconnectNodes.add(s);
      }
    });

    reconnectNodes.forEach(s -> {
      Channel ch = connectionMap.remove(s);
      if (ch != null) {
        ch.disconnect();
        ch.close();
      }
      connect(address(s));
    });
  }

  private List<String> buildNodeList() {
    List<String> l = zkClient.list(NODE_LIST_PATH);
    if (l == null) {
      return buildNodeList();
    } else {
      return l;
    }
  }

  private InetSocketAddress address(String node) {
    String chunks[] = node.split(":");
    return new InetSocketAddress(chunks[0], Integer.parseInt(chunks[1]));
  }

  private void refreshPool() {
    buildNodeList().stream().forEach(xs -> {
      connect(address(xs));
    });

    try {
      connectCheck.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {
          checkConnection();
        }
      }, 1000, 5000, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public ListenableFuture<Channel> getNode(String node) throws ChicagoClientTimeoutException, InterruptedException {
//    log.debug("Trying to get node:"+node);
//    return _getNode(node, System.currentTimeMillis());

    SettableFuture f = SettableFuture.create();
    _getNode(f, node);

    return f;
  }

  //  private ChannelFuture _getNode(String node, long startTime) throws ChicagoClientTimeoutException, InterruptedException {
  private void _getNode(SettableFuture<Channel> f, String node) throws ChicagoClientTimeoutException, InterruptedException {

  while (!connectionMap.containsKey(node)) {
//      if (TIMEOUT_ENABLED && (System.currentTimeMillis() - startTime) > TIMEOUT) {
//        Thread.currentThread().interrupt();
//        System.out.println("Cannot get connection for node "+ node +" connectionMap ="+ connectionMap.keySet().toString());
//        throw new ChicagoClientTimeoutException();
//      }
//      try {
//        Thread.sleep(0, 250);
//      } catch (InterruptedException e) {
//        e.printStackTrace();
//      }
    }

    if (connectionMap.containsKey(node)) {
      Channel ch = connectionMap.get(node);
      if (ch.isWritable()) {
        f.set(ch);
      } else {
        checkConnection();
        _getNode(f, node, 0);
      }
    } else {
      Thread.sleep(0, 250);
      checkConnection();
      _getNode(f, node, 0);
    }
  }


  private void _getNode(SettableFuture<Channel> f, String node, int retry) throws ChicagoClientTimeoutException, InterruptedException {

    if (retry > MAX_RETRY) {
      if (connectionMap.containsKey(node)) {
        Channel ch = connectionMap.get(node);
        if (ch.isWritable()) {
          f.set(ch);
        } else {
          checkConnection();
          _getNode(f, node, retry++);
        }
      } else {
        Thread.sleep(0, 250);
        checkConnection();
        _getNode(f, node, retry++);
      }
    } else {
      f.setException(new ChicagoClientException("Could not get node from the Channel Pool"));
      log.error("Could not get node from the Channel Pool");
    }
  }

  public void addNode(String hostname, ChannelFuture future) {
    if (future.isSuccess()) {
      connectionMap.put(hostname, future.channel());
    }
  }

  private void connect(InetSocketAddress server) {
    // Start the connection attempt.
    Bootstrap bootstrap = new Bootstrap();
    bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 500)
        .option(ChannelOption.SO_REUSEADDR, true)
        .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
        .option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32 * 1024)
        .option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024)
        .option(ChannelOption.TCP_NODELAY, true);
    bootstrap.group(workerLoop)
        .channel(NioSocketChannel.class)
        .handler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel channel) throws Exception {
            ChannelPipeline cp = channel.pipeline();
            cp.addLast(new XioSecurityHandlerImpl(true).getEncryptionHandler());
            //cp.addLast(new XioIdleDisconnectHandler(20, 20, 20));
            cp.addLast(new ChicagoClientCodec());
            cp.addLast(handler);
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

    connect2(server, bootstrap, retryLoop);
  }

  private void connect2(InetSocketAddress server, Bootstrap bootstrap, RetryLoop retryLoop) {
    ChannelFutureListener listener = new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) {
        if (!future.isSuccess()) {
          if (!running.get()) {
            return;
          }
          try {
            retryLoop.takeException((Exception) future.cause());
            log.info("==== Service connect failure (will retry)", future.cause());
            connect2(server, bootstrap, retryLoop);
          } catch (Exception e) {
            log.error("==== Service connect failure ", future.cause());
            // Close the connection if the connection attempt has failed.
            future.channel().close();
          }
        } else {
          log.debug("Chicago connected to: " + server);
          String hostname = server.getAddress().getHostAddress();
          if (hostname.equals("localhost")) {
            hostname = "127.0.0.1";
          }
          log.debug("Adding hostname: " + hostname + ":" + ((InetSocketAddress) future.channel().remoteAddress()).getPort());
          addNode(hostname + ":" + ((InetSocketAddress) future.channel().remoteAddress()).getPort(), future);
        }
      }
    };

    bootstrap.connect(server).addListener(listener);
  }

  public void releaseChannel(String node, Channel cf1) {

  }
}
