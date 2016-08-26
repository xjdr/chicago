package com.xjeffrose.chicago;

import com.google.common.collect.ImmutableList;
import com.google.common.hash.Funnels;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.xjeffrose.chicago.client.ChicagoClientHandler;
import com.xjeffrose.chicago.client.ClientNodeWatcher;
import com.xjeffrose.chicago.client.ConnectionPoolManager;
import com.xjeffrose.chicago.client.ConnectionPoolManagerImpl;
import com.xjeffrose.chicago.client.EmbeddedConnectionManager;
import io.netty.channel.ChannelHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.internal.PlatformDependent;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ChicagoPaxosClient {
  private final static String REPLICATION_LOCK_PATH = "/chicago/replication-lock";
  private final static String NODE_LIST_PATH = "/chicago/node-list";
  private final static long TIMEOUT = 3000;

  private final ZkClient zkClient;
  private final int replicaSize;
  private final Map<UUID, SettableFuture<byte[]>> futureMap;
  private final ChannelHandler handler;
  private final NioEventLoopGroup workerLoop = new NioEventLoopGroup(5,
      new ThreadFactoryBuilder()
          .setNameFormat("chicagoClient-nioEventLoopGroup-%d")
          .build()
  );
  private ClientNodeWatcher clientNodeWatcher;
  private ConnectionPoolManager connectionManager;
  private RendezvousHash<String> rendezvousHash;
  private boolean singleServer = false;
  private String singleServerAddr = null;
  private EmbeddedChannel ech = null;
  private CountDownLatch latch;


  public ChicagoPaxosClient(String addr) {
    this.zkClient = null;
    this.futureMap = PlatformDependent.newConcurrentHashMap();
    this.handler = new ChicagoClientHandler(futureMap);
    this.singleServer = true;
    this.singleServerAddr = addr;
    this.clientNodeWatcher = null;
    this.replicaSize = 1;
  }

  public ChicagoPaxosClient(String zkConnectionString, int replicaSize) {
    this.zkClient = new ZkClient(zkConnectionString, false);
    this.replicaSize = replicaSize;
    this.futureMap = PlatformDependent.newConcurrentHashMap();
    this.handler = new ChicagoClientHandler(futureMap);
    this.singleServer = false;
  }

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
        rendezvousHash = new RendezvousHash(Funnels.stringFunnel(Charset.defaultCharset()), ImmutableList.of(ech.toString()), replicaSize);
      } else {
        List<String> nodeList;
        if (singleServer) {
          nodeList = new ArrayList<>();
          nodeList.add(singleServerAddr);
          connectionManager = new ConnectionPoolManagerImpl(nodeList, handler, workerLoop);
          rendezvousHash = new RendezvousHash(Funnels.stringFunnel(Charset.defaultCharset()), nodeList, replicaSize);
          connectionManager.start();
        } else {
          nodeList = buildNodeList();
          connectionManager = new ConnectionPoolManagerImpl(nodeList, handler, workerLoop);
          connectionManager.start();
          rendezvousHash = new RendezvousHash(Funnels.stringFunnel(Charset.defaultCharset()), nodeList, replicaSize);
          clientNodeWatcher = new ClientNodeWatcher(zkClient);
          clientNodeWatcher.start();
          clientNodeWatcher.registerListener((NodeListener) rendezvousHash);
          clientNodeWatcher.registerListener((NodeListener) connectionManager);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Cannot start connection manager");
    }

  }

  protected List<String> buildNodeList() {
    return zkClient.list(NODE_LIST_PATH);
  }


  public List<String> getEffectiveNodes(byte[] key) {
    List<String> hashList = new ArrayList<>(rendezvousHash.get(key));
    if (!singleServer && !(clientNodeWatcher == null)) {
      String path = REPLICATION_LOCK_PATH + "/" + new String(key);
      List<String> replicationList = clientNodeWatcher.getReplicationPathData(path);
      hashList.removeAll(replicationList);
    }

    return hashList;
  }


  public ListenableFuture<List<byte[]>> write(byte[] key, byte[] value) {
    return write(ChiUtil.defaultColFam.getBytes(), key, value);
  }

  public ListenableFuture<List<byte[]>> write(byte[] colFam, byte[] key, byte[] val) {
    List<String> nodes = getEffectiveNodes(colFam);
    final List<SettableFuture<byte[]>> futureList = new ArrayList<>();
    for (int i = 1; i < replicaSize; i++) {

      UUID id = UUID.randomUUID();
      SettableFuture<byte[]> f = SettableFuture.create();
      futureMap.put(id, f);
      futureList.add(f);
      int finalI = i;
      Futures.addCallback(f, new FutureCallback<byte[]>() {
        @Override
        public void onSuccess(@Nullable byte[] bytes) {
          f.set(bytes);
        }

        @Override
        public void onFailure(Throwable throwable) {

          Futures.addCallback(connectionManager.write(nodes.get(finalI), new DefaultChicagoMessage(id, Op.MPAXOS_PROPOSE_WRITE, colFam, key, val)), new FutureCallback<byte[]>() {
            @Override
            public void onSuccess(@Nullable byte[] bytes) {
              f.set(bytes);
            }

            @Override
            public void onFailure(Throwable throwable) {
              f.setException(throwable);
            }
          });
        }
      });
    }

    return Futures.allAsList(futureList);

  }

  public ListenableFuture<List<byte[]>> tsWrite(byte[] topic, byte[] key, byte[] val) {
    List<String> nodes = getEffectiveNodes(topic);
    final List<SettableFuture<byte[]>> futureList = new ArrayList<>();
    for (int i = 1; i < replicaSize; i++) {

      UUID id = UUID.randomUUID();
      SettableFuture<byte[]> f = SettableFuture.create();
      futureMap.put(id, f);
      futureList.add(f);
      int finalI = i;
      Futures.addCallback(f, new FutureCallback<byte[]>() {
        @Override
        public void onSuccess(@Nullable byte[] bytes) {
          f.set(bytes);
        }

        @Override
        public void onFailure(Throwable throwable) {

          Futures.addCallback(connectionManager.write(nodes.get(finalI), new DefaultChicagoMessage(id, Op.MPAXOS_PROPOSE_TS_WRITE, topic, key, val)), new FutureCallback<byte[]>() {
            @Override
            public void onSuccess(@Nullable byte[] bytes) {
              f.set(bytes);
            }

            @Override
            public void onFailure(Throwable throwable) {
              f.setException(throwable);
            }
          });
        }
      });
    }

    return Futures.allAsList(futureList);
  }

  public ListenableFuture<List<byte[]>> tsWrite(byte[] topic, byte[] val) {
    return tsWrite(topic, null, val);
  }

  public int getReplicaSize() {
    return replicaSize;
  }
}
