package com.xjeffrose.chicago.client;

import com.google.common.hash.Funnels;
import com.xjeffrose.chicago.DefaultChicagoMessage;
import com.xjeffrose.chicago.Op;
import com.xjeffrose.chicago.ZkClient;
import com.xjeffrose.xio.SSL.XioSecurityHandlerImpl;
import com.xjeffrose.xio.client.retry.BoundedExponentialBackoffRetry;
import com.xjeffrose.xio.client.retry.RetryLoop;
import com.xjeffrose.xio.client.retry.TracerDriver;
import com.xjeffrose.xio.core.XioIdleDisconnectHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.log4j.Logger;


public class ChicagoClient {
  private static final Logger log = Logger.getLogger(ChicagoClient.class);
  private final static String NODE_LIST_PATH = "/chicago/node-list";

  private final InetSocketAddress single_server;
  private final RendezvousHash rendezvousHash;
  private final ClientNodeWatcher clientNodeWatcher;
  private final ZkClient zkClient;
  private final ConnectionPoolManager connectionPoolMgr;
//  private final Map<String, Listener> listenerMap = new HashMap<>();
//  private final Map<String, ChannelFuture> connectionPool = new HashMap<>();


  ChicagoClient(InetSocketAddress server) {

    this.single_server = server;
    this.zkClient = null;
    this.rendezvousHash = null;
    this.clientNodeWatcher = null;
    connectionPoolMgr = null;
  }

  public ChicagoClient(String zkConnectionString) throws InterruptedException {

    this.single_server = null;
    this.zkClient = new ZkClient(zkConnectionString);
    zkClient.start();

    this.rendezvousHash = new RendezvousHash(Funnels.stringFunnel(Charset.defaultCharset()), buildNodeList());
    this.clientNodeWatcher = new ClientNodeWatcher();
    clientNodeWatcher.refresh(zkClient, rendezvousHash);
    this.connectionPoolMgr = new ConnectionPoolManager(zkClient);
//    refreshPool();
  }


  private List<String> buildNodeList() {
    return zkClient.list(NODE_LIST_PATH);
  }

  public byte[] read(byte[] key) {
    List<byte[]> responseList = new ArrayList<>();

    if (single_server != null) {
    }

    rendezvousHash.get(key).forEach(xs -> {
      ChannelFuture cf = connectionPoolMgr.getNode((String) xs);
      if (cf.channel().isWritable()) {
        cf.channel().writeAndFlush(new DefaultChicagoMessage(Op.READ, key, null));
        responseList.add((byte[]) connectionPoolMgr.getListener((String) xs).getResponse());
      }

    });

    return responseList.stream().findFirst().orElse(null);
  }

  public boolean write(byte[] key, byte[] value) {
    List<Boolean> responseList = new ArrayList<>();

    if (single_server != null) {
//      connect(single_server, Op.WRITE, key, value, listener);
    }

      rendezvousHash.get(key).forEach(xs -> {
        ChannelFuture cf = connectionPoolMgr.getNode((String) xs);
        if (cf.channel().isWritable()) {
          cf.channel().writeAndFlush(new DefaultChicagoMessage(Op.WRITE, key, value));
          responseList.add(connectionPoolMgr.getListener((String) xs).getStatus());
        }

      });

    return responseList.stream().allMatch(b -> b);
  }

  public boolean delete(byte[] key) {
    List<Boolean> responseList = new ArrayList<>();

//    if (single_server != null) {
////      connect(single_server, Op.DELETE, key, value, listener);
//    } else {
////      if (connectionPool.size() == 0) {
//        try {
//          Thread.sleep(200);
//          return delete(key);
//        } catch (InterruptedException e) {
//          log.error(e);
//        }
//      }
//      rendezvousHash.get(key).forEach(xs -> {
////        ChannelFuture cf = connectionPool.get(xs);
//        if (cf.channel().isWritable()) {
//          cf.channel().writeAndFlush(new DefaultChicagoMessage(Op.DELETE, key, null));
//          responseList.add(listenerMap.get(xs).getStatus());
//        } else {
//          delete(key);
//        }
//      });
//    }
//
//    return responseList.stream().allMatch(b -> b);

    return false;
  }


//  private void connect(InetSocketAddress server, Listener listener) {
//    // Start the connection attempt.
//    Bootstrap bootstrap = new Bootstrap();
//    bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 500)
//        .option(ChannelOption.SO_REUSEADDR, true)
//        .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
//        .option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32 * 1024)
//        .option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024)
//        .option(ChannelOption.TCP_NODELAY, true);
//    bootstrap.group(new NioEventLoopGroup(20))
//        .channel(NioSocketChannel.class)
//        .handler(new ChannelInitializer<SocketChannel>() {
//          @Override
//          protected void initChannel(SocketChannel channel) throws Exception {
//            ChannelPipeline cp = channel.pipeline();
//            cp.addLast(new XioSecurityHandlerImpl(true).getEncryptionHandler());
////            cp.addLast(new XioSecurityHandlerImpl(true).getAuthenticationHandler());
//            cp.addLast(new XioIdleDisconnectHandler(60, 60, 60));
//            cp.addLast(new ChicagoClientCodec());
//            cp.addLast(new ChicagoClientHandler(listener));
//          }
//        });
//
//    BoundedExponentialBackoffRetry retry = new BoundedExponentialBackoffRetry(50, 500, 4);
//
//    TracerDriver tracerDriver = new TracerDriver() {
//
//      @Override
//      public void addTrace(String name, long time, TimeUnit unit) {
//      }
//
//      @Override
//      public void addCount(String name, int increment) {
//      }
//    };
//
//    RetryLoop retryLoop = new RetryLoop(retry, new AtomicReference<>(tracerDriver));
//    connect2(server, bootstrap, retryLoop);
//  }
//
//  private void connect2(InetSocketAddress server, Bootstrap bootstrap, RetryLoop retryLoop) {
//    ChannelFutureListener listener = new ChannelFutureListener() {
//      @Override
//      public void operationComplete(ChannelFuture future) {
//        if (!future.isSuccess()) {
//          try {
//            retryLoop.takeException((Exception) future.cause());
//            log.error("==== Service connect failure (will retry)", future.cause());
//            connect2(server, bootstrap, retryLoop);
//          } catch (Exception e) {
//            log.error("==== Service connect failure ", future.cause());
//            // Close the connection if the connection attempt has failed.
//            future.channel().close();
//          }
//        } else {
//          log.debug("Chicago connected: ");
//          String hostname = ((InetSocketAddress) future.channel().remoteAddress()).getHostName();
//          if (hostname.equals("localhost")) {
//            hostname = "127.0.0.1";
//          }
//          connectionPoolMgr.addNode(hostname, future);
//        }
//      }
//    };
//
//    bootstrap.connect(server).addListener(listener);
//  }
}
