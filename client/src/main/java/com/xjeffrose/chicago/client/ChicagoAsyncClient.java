package com.xjeffrose.chicago.client;

import com.google.common.collect.ImmutableList;
import com.google.common.hash.Funnels;
import com.google.common.util.concurrent.*;
import com.xjeffrose.chicago.*;
import io.netty.channel.ChannelHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.internal.PlatformDependent;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public class ChicagoAsyncClient implements Client {
  private final static String REPLICATION_LOCK_PATH = "/chicago/replication-lock";
  private final static String NODE_LIST_PATH = "/chicago/node-list";
  private final static long TIMEOUT = 3000;

  private final ZkClient zkClient;
  private final Map<UUID, SettableFuture<byte[]>> futureMap;
  private final ChannelHandler handler;
  private ClientNodeWatcher clientNodeWatcher;
  private final NioEventLoopGroup workerLoop = new NioEventLoopGroup(5,
      new ThreadFactoryBuilder()
          .setNameFormat("chicagoClient-nioEventLoopGroup-%d")
          .build()
  );

  private ConnectionPoolManager connectionManager;
  private RendezvousHash<String> rendezvousHash;
  private int quorum = 3;
  private boolean singleServer = false;
  private String singleServerAddr = null;
  private EmbeddedChannel ech = null;
  private CountDownLatch latch;


  public ChicagoAsyncClient(String addr) {
    this.zkClient = null;
    this.futureMap = PlatformDependent.newConcurrentHashMap();
    this.handler = new ChicagoClientHandler(futureMap);
    this.singleServer = true;
    this.singleServerAddr = addr;
    this.clientNodeWatcher=null;
    this.quorum = 1;
  }

  public ChicagoAsyncClient(String zkConnectionString, int q) {
    this.zkClient = new ZkClient(zkConnectionString, false);
    this.futureMap = PlatformDependent.newConcurrentHashMap();
    this.handler = new ChicagoClientHandler(futureMap);
    this.quorum = q;
    this.singleServer = false;
  }

  ChicagoAsyncClient(EmbeddedChannel ech, Map<UUID, SettableFuture<byte[]>> futureMap, int q) {
    // This constructor is for testing only
    this.ech = ech;
    this.zkClient = null;
    this.futureMap = futureMap;
    this.handler = new ChicagoClientHandler(futureMap);
    this.quorum = q;
    this.singleServer = false;
  }

  @Override
  public void start() {
    if (zkClient != null) {
      try {
        zkClient.start();
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Cannot instantiate ZKClient!!");
      }
    }

    try {
      if (ech != null) {
        connectionManager = new EmbeddedConnectionManager(ech);
        rendezvousHash = new RendezvousHash(Funnels.stringFunnel(Charset.defaultCharset()), ImmutableList.of(ech.toString()), quorum);
      } else {
        List<String> nodeList;
        if (singleServer) {
          nodeList = new ArrayList<>();
          nodeList.add(singleServerAddr);
          connectionManager = new ConnectionPoolManagerImpl(nodeList, handler, workerLoop);
          rendezvousHash = new RendezvousHash(Funnels.stringFunnel(Charset.defaultCharset()), nodeList, quorum);
          connectionManager.start();
        } else {
          nodeList = buildNodeList();
          connectionManager = new ConnectionPoolManagerImpl(nodeList, handler, workerLoop);
          connectionManager.start();
          rendezvousHash = new RendezvousHash(Funnels.stringFunnel(Charset.defaultCharset()), nodeList, quorum);
          clientNodeWatcher = new ClientNodeWatcher(zkClient);
          clientNodeWatcher.start();
          clientNodeWatcher.registerListener((NodeListener) rendezvousHash);
          clientNodeWatcher.registerListener((NodeListener) connectionManager);
        }
      }
    } catch (Exception e){
      e.printStackTrace();
      throw new RuntimeException("Cannot start connection manager");
    }

  }

  protected List<String> buildNodeList() {
    return zkClient.list(NODE_LIST_PATH);
  }

  public List<String> getNodeList(byte[] key) {
    return rendezvousHash.get(key);
  }

  public List<String> getEffectiveNodes(byte[] key){
    List<String> hashList = new ArrayList<>(rendezvousHash.get(key));
    if(!singleServer && !(clientNodeWatcher == null)) {
      String path = REPLICATION_LOCK_PATH + "/" + new String(key);
      List<String> replicationList = clientNodeWatcher.getReplicationPathData(path);
      hashList.removeAll(replicationList);
    }

    return hashList;
  }

  public List<String> scanColFamily() throws Exception {
    List<String> resp = new ArrayList<>();
    if(this.zkClient != null) {
      resp = this.zkClient.list(REPLICATION_LOCK_PATH);
    }
    return resp;
  }

  @Override
  public ListenableFuture<byte[]> scanKeys(byte[] colFam) {
    List<String> nodes = getEffectiveNodes(colFam);
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

    Futures.addCallback(connectionManager.write(nodes.get(0), new DefaultChicagoMessage(id, Op.SCAN_KEYS, colFam, null, null)), new FutureCallback<Boolean>() {
      @Override
      public void onSuccess(@Nullable Boolean aBoolean) {

      }

      @Override
      public void onFailure(Throwable throwable) {

      }
    });

    workerLoop.schedule(() -> {
      if (nodes.size() > 1) {
        Futures.addCallback(connectionManager.write(nodes.get(1), new DefaultChicagoMessage(id, Op.SCAN_KEYS, colFam, null, null)), new FutureCallback<Boolean>() {
          @Override
          public void onSuccess(@Nullable Boolean aBoolean) {

          }

          @Override
          public void onFailure(Throwable throwable) {
            try {
              f.set(scanKeys(colFam).get(TIMEOUT, TimeUnit.MILLISECONDS));
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
              f.setException(throwable);
            }
          }
        });
      }
    }, 2, TimeUnit.MILLISECONDS);

    return f;
  }


  public ListenableFuture<byte[]> read(byte[] key) {
    return read(ChiUtil.defaultColFam.getBytes(), key);
  }

  @Override
  public ListenableFuture<byte[]> read(byte[] colFam, byte[] key) {
    List<String> nodes = getEffectiveNodes(colFam);
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

  public ListenableFuture<Boolean> write(byte[] key, byte[] value) {
    return write(ChiUtil.defaultColFam.getBytes(), key, value);
  }

  @Override
  public ListenableFuture<Boolean> write(byte[] colFam, byte[] key, byte[] val) {
    final List<SettableFuture<byte[]>> futureList = new ArrayList<>();
    final SettableFuture<Boolean> respFuture = SettableFuture.create();
    final List<String> nodes = getEffectiveNodes(colFam);
    if (nodes.size() < quorum) {
      log.error("Unable to establish Quorum");
      return null;
    }
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

  public ListenableFuture<byte[]> tsWrite(byte[] topic, byte[] key,  byte[] val) {
    final List<SettableFuture<byte[]>> futureList = new ArrayList<>();
    final SettableFuture<byte[]> respFuture = SettableFuture.create();
    final List<String> nodes = getEffectiveNodes(topic);
    if (nodes.size() < quorum) {
      log.error("Unable to establish Quorum");
      return null;
    }
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

      Futures.addCallback(connectionManager.write(xs, new DefaultChicagoMessage(id, Op.TS_WRITE, topic, key, val)), new FutureCallback<Boolean>() {
        @Override
        public void onSuccess(@Nullable Boolean aBoolean) {
        }

        @Override
        public void onFailure(Throwable throwable) {
          Futures.addCallback(connectionManager.write(xs, new DefaultChicagoMessage(id, Op.TS_WRITE, topic, key, val)), new FutureCallback<Boolean>() {
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
          respFuture.set(tsWrite(topic,key, val).get(TIMEOUT, TimeUnit.MILLISECONDS));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
          respFuture.setException(e);
        }
      }
    });

    return respFuture;
  }

  @Override
  public ListenableFuture<byte[]> tsWrite(byte[] topic, byte[] val) {
    return tsWrite(topic,null,val);
  }

  @Override
  public ListenableFuture<byte[]> stream(byte[] topic, byte[] offset) {
    List<String> nodes = getEffectiveNodes(topic);
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
  public ListenableFuture<Boolean> deleteColFam(byte[] colFam) {
    List<SettableFuture<byte[]>> futureList = new ArrayList<>();
    SettableFuture<Boolean> respFuture = SettableFuture.create();
    List<String> nodes = getEffectiveNodes(colFam);
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

      Futures.addCallback(connectionManager.write(xs, new DefaultChicagoMessage(id, Op.DELETE, colFam, null, null)), new FutureCallback<Boolean>() {
        @Override
        public void onSuccess(@Nullable Boolean aBoolean) {
          futureList.add(f);
        }

        @Override
        public void onFailure(Throwable throwable) {
          Futures.addCallback(connectionManager.write(xs, new DefaultChicagoMessage(id, Op.DELETE, colFam, null, null)), new FutureCallback<Boolean>() {
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
          respFuture.set(deleteColFam(colFam).get(TIMEOUT, TimeUnit.MILLISECONDS));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
          respFuture.setException(e);
        }
      }
    });

    return respFuture;
  }

  @Override
  public void close() {
      workerLoop.shutdownGracefully();
  }
}
