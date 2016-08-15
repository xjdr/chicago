package com.xjeffrose.chicago.client;

import com.google.common.collect.ImmutableList;
import com.google.common.hash.Funnels;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.xjeffrose.chicago.DefaultChicagoMessage;
import com.xjeffrose.chicago.Op;
import com.xjeffrose.chicago.ZkClient;
import io.netty.channel.ChannelHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.internal.PlatformDependent;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ChicagoAsyncClient implements Client {
  private final static String REPLICATION_LOCK_PATH = "/chicago/replication-lock";
  private final static String NODE_LIST_PATH = "/chicago/node-list";
  private final static long TIMEOUT = 3000;

  private final ZkClient zkClient;
  private final Map<UUID, SettableFuture<byte[]>> futureMap;
  private final ChannelHandler handler;
  private final NioEventLoopGroup workerLoop = new NioEventLoopGroup(5,
      new ThreadFactoryBuilder()
          .setNameFormat("chicagoClient-nioEventLoopGroup-%d")
          .build()
  );

  private ConnectionPoolManager connectionManager;
  private RendezvousHash<String> rendezvousHash;
  private int quorum = 3;
  private EmbeddedChannel ech = null;


  public ChicagoAsyncClient(InetSocketAddress addr) {
    this(addr, 3);
  }

  public ChicagoAsyncClient(InetSocketAddress addr, int q) {
    this.zkClient = null;
    this.futureMap = PlatformDependent.newConcurrentHashMap();
    this.handler = new ChicagoClientHandler(futureMap);
    this.quorum = q;
  }

  public ChicagoAsyncClient(String zkConnectionString) {
    this(zkConnectionString, 3);
  }

  public ChicagoAsyncClient(String zkConnectionString, int q) {
    this.zkClient = new ZkClient(zkConnectionString, false);
    this.futureMap = PlatformDependent.newConcurrentHashMap();
    this.handler = new ChicagoClientHandler(futureMap);
    this.quorum = q;
  }

  ChicagoAsyncClient(EmbeddedChannel ech, Map<UUID, SettableFuture<byte[]>> futureMap, int q) {
    // This constructor is for testing only
    this.ech = ech;
    this.zkClient = null;
    this.futureMap = futureMap;
    this.handler = new ChicagoClientHandler(futureMap);
    this.quorum = q;

  }

  @Override
  public void start() {
    if (zkClient != null) {
      try {
        zkClient.start();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    if (ech != null) {
      connectionManager = new EmbeddedConnectionManager(ech);
      rendezvousHash = new RendezvousHash(Funnels.stringFunnel(Charset.defaultCharset()), ImmutableList.of(ech.toString()), 3);
    } else {
      List<String> nodeList = buildNodeList();
      connectionManager = new ConnectionPoolManagerImpl(nodeList, handler, workerLoop);
      connectionManager.start();
      rendezvousHash = new RendezvousHash(Funnels.stringFunnel(Charset.defaultCharset()), nodeList, 3);
    }

  }

  protected List<String> buildNodeList() {
    return zkClient.list(NODE_LIST_PATH);
  }


  @Override
  public ListenableFuture<byte[]> read(byte[] colFam, byte[] key) {
    List<String> nodes = rendezvousHash.get(colFam);
    UUID id = UUID.randomUUID();
    SettableFuture<byte[]> f = SettableFuture.create();
    futureMap.put(id, f);
    Futures.addCallback(f, new FutureCallback<byte[]>() {
      @Override
      public void onSuccess(@Nullable byte[] bytes) {
        f.set(bytes);
      }

      @Override
      public void onFailure(Throwable throwable) {
        f.setException(throwable);
      }
    });

    Futures.addCallback(connectionManager.write(nodes.get(0), new DefaultChicagoMessage(id, Op.READ, colFam, key, null)), new FutureCallback<Boolean>() {
      @Override
      public void onSuccess(@Nullable Boolean aBoolean) {

      }

      @Override
      public void onFailure(Throwable throwable) {

      }
    });

    workerLoop.schedule(() -> {
      if (nodes.size() > 1) {
        Futures.addCallback(connectionManager.write(nodes.get(1), new DefaultChicagoMessage(id, Op.READ, colFam, key, null)), new FutureCallback<Boolean>() {
          @Override
          public void onSuccess(@Nullable Boolean aBoolean) {

          }

          @Override
          public void onFailure(Throwable throwable) {
            try {
              f.set(read(colFam, key).get(TIMEOUT, TimeUnit.MILLISECONDS));
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
              f.setException(throwable);
            }
          }
        });
      }
    }, 2, TimeUnit.MILLISECONDS);

    return f;
  }

  @Override
  public ListenableFuture<Boolean> write(byte[] colFam, byte[] key, byte[] val) {
    List<SettableFuture<byte[]>> futureList = new ArrayList<>();
    SettableFuture<Boolean> respFuture = SettableFuture.create();
    List<String> nodes = rendezvousHash.get(colFam);
    nodes.stream().forEach(xs -> {
      UUID id = UUID.randomUUID();
      SettableFuture<byte[]> f = SettableFuture.create();
      futureMap.put(id, f);
      Futures.addCallback(f, new FutureCallback<byte[]>() {
        @Override
        public void onSuccess(@Nullable byte[] bytes) {
          f.set(bytes);
        }

        @Override
        public void onFailure(Throwable throwable) {
          f.setException(throwable);
        }
      });

      Futures.addCallback(connectionManager.write(xs, new DefaultChicagoMessage(id, Op.WRITE, colFam, key, val)), new FutureCallback<Boolean>() {
        @Override
        public void onSuccess(@Nullable Boolean aBoolean) {
          futureList.add(f);
        }

        @Override
        public void onFailure(Throwable throwable) {
          Futures.addCallback(connectionManager.write(xs, new DefaultChicagoMessage(id, Op.WRITE, colFam, key, val)), new FutureCallback<Boolean>() {
            @Override
            public void onSuccess(@Nullable Boolean aBoolean) {
              futureList.add(f);
            }

            @Override
            public void onFailure(Throwable throwable) {

            }
          });
        }
      });
    });

    Futures.addCallback(Futures.successfulAsList(futureList), new FutureCallback<List<byte[]>>() {
      @Override
      public void onSuccess(@Nullable List<byte[]> bytes) {
        respFuture.set(true);
      }

      @Override
      public void onFailure(Throwable throwable) {
        try {
          respFuture.set(write(colFam, key, val).get(TIMEOUT, TimeUnit.MILLISECONDS));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
          respFuture.setException(e);
        }
      }
    });

    return respFuture;
  }

  @Override
  public ListenableFuture<byte[]> tsWrite(byte[] topic, byte[] val) {

    List<SettableFuture<byte[]>> futureList = new ArrayList<>();
    SettableFuture<byte[]> respFuture = SettableFuture.create();
    List<String> nodes = rendezvousHash.get(topic);
    nodes.stream().forEach(xs -> {
      UUID id = UUID.randomUUID();
      SettableFuture<byte[]> f = SettableFuture.create();
      futureMap.put(id, f);
      futureList.add(f);

      Futures.addCallback(f, new FutureCallback<byte[]>() {
        @Override
        public void onSuccess(@Nullable byte[] bytes) {
          f.set(bytes);
        }

        @Override
        public void onFailure(Throwable throwable) {
          f.setException(throwable);
        }
      });

      Futures.addCallback(connectionManager.write(xs, new DefaultChicagoMessage(id, Op.TS_WRITE, topic, null, val)), new FutureCallback<Boolean>() {
        @Override
        public void onSuccess(@Nullable Boolean aBoolean) {
        }

        @Override
        public void onFailure(Throwable throwable) {
          Futures.addCallback(connectionManager.write(xs, new DefaultChicagoMessage(id, Op.TS_WRITE, topic, null, val)), new FutureCallback<Boolean>() {
            @Override
            public void onSuccess(@Nullable Boolean aBoolean) {

            }

            @Override
            public void onFailure(Throwable throwable) {

            }
          });
        }
      });
    });

    Futures.addCallback(Futures.successfulAsList(futureList), new FutureCallback<List<byte[]>>() {
      @Override
      public void onSuccess(@Nullable List<byte[]> bytes) {
        respFuture.set(bytes.get(0));
      }

      @Override
      public void onFailure(Throwable throwable) {
        try {
          respFuture.set(tsWrite(topic, val).get(TIMEOUT, TimeUnit.MILLISECONDS));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
          respFuture.setException(e);
        }
      }
    });

    return respFuture;
  }

  @Deprecated
  @Override
  public ListenableFuture<byte[]> batchWrite(byte[] topic, byte[] val) {
    return null;
  }

  @Override
  public ListenableFuture<byte[]> stream(byte[] topic, byte[] offset) {
    List<String> nodes = rendezvousHash.get(topic);
    UUID id = UUID.randomUUID();
    SettableFuture<byte[]> f = SettableFuture.create();
    futureMap.put(id, f);
    Futures.addCallback(f, new FutureCallback<byte[]>() {
      @Override
      public void onSuccess(@Nullable byte[] bytes) {
        f.set(bytes);
      }

      @Override
      public void onFailure(Throwable throwable) {
        f.setException(throwable);
      }
    });

    Futures.addCallback(connectionManager.write(nodes.get(0), new DefaultChicagoMessage(id, Op.STREAM, topic, null, offset)), new FutureCallback<Boolean>() {
      @Override
      public void onSuccess(@Nullable Boolean aBoolean) {

      }

      @Override
      public void onFailure(Throwable throwable) {

      }
    });

    workerLoop.schedule(() -> {
      if (nodes.size() > 1) {
        Futures.addCallback(connectionManager.write(nodes.get(1), new DefaultChicagoMessage(id, Op.STREAM, topic, null, offset)), new FutureCallback<Boolean>() {
          @Override
          public void onSuccess(@Nullable Boolean aBoolean) {

          }

          @Override
          public void onFailure(Throwable throwable) {
            try {
              f.set(stream(topic, offset).get(TIMEOUT, TimeUnit.MILLISECONDS));
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
              f.setException(throwable);
            }
          }
        });
      }
    }, 2, TimeUnit.MILLISECONDS);

    return f;

  }

  @Override
  public void close() throws Exception {
//    zkClient.stop();
  }
}
