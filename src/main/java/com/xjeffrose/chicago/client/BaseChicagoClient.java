package com.xjeffrose.chicago.client;

import com.google.common.hash.Funnels;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.SettableFuture;
import com.xjeffrose.chicago.ZkClient;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for Chicago clients
 */
abstract public class BaseChicagoClient {
  private static final Logger log = LoggerFactory.getLogger(BaseChicagoClient.class);
  protected final static String NODE_LIST_PATH = "/chicago/node-list";
  public final static String REPLICATION_LOCK_PATH ="/chicago/replication-lock";
  protected static final long TIMEOUT = 1000;
  protected static boolean TIMEOUT_ENABLED = true;
  protected static int MAX_RETRY = 3;
  protected final AtomicInteger nodesAvailable = new AtomicInteger(0);

  protected final boolean single_server;
  protected final RendezvousHash rendezvousHash;
  protected final ClientNodeWatcher clientNodeWatcher;
  private CountDownLatch latch;
  private final ClientNodeWatcher.Listener listener = new ClientNodeWatcher.Listener() {
      public void nodeAdded() {
        int avail = nodesAvailable.incrementAndGet();
        if (latch != null) {
          latch.countDown();
        }
      }
      public void nodeRemoved() {
        nodesAvailable.decrementAndGet();
      }
  };
  protected final ZkClient zkClient;
  protected final ConnectionPoolManager connectionPoolMgr;
  protected int quorum;

  protected Map<UUID, SettableFuture<byte[]>> futureMap = new ConcurrentHashMap<>();
  protected EventLoopGroup evg;


  public BaseChicagoClient(String address){
    this.single_server = true;
    this.zkClient = null;
    this.quorum = 1;
    ArrayList<String> nodeList = new ArrayList<>();
    nodeList.add(address);
    this.rendezvousHash =  new RendezvousHash(Funnels.stringFunnel(Charset.defaultCharset()), nodeList, quorum);
    clientNodeWatcher = null;
    this.connectionPoolMgr = new ConnectionPoolManager(address, futureMap);
    if (Epoll.isAvailable()) {
      evg = new EpollEventLoopGroup(5);
    } else {
      evg = new NioEventLoopGroup(5);
    }
  }

  public BaseChicagoClient(String zkConnectionString, int quorum) throws InterruptedException {

    this.single_server = false;
    this.zkClient = new ZkClient(zkConnectionString, false);
    this.quorum = quorum;

    ArrayList<String> nodeList = new ArrayList<>();
    this.rendezvousHash = new RendezvousHash(Funnels.stringFunnel(Charset.defaultCharset()), nodeList, quorum);
    clientNodeWatcher = new ClientNodeWatcher(zkClient, rendezvousHash, listener);
    this.connectionPoolMgr = new ConnectionPoolManager(zkClient, futureMap);
    if (Epoll.isAvailable()) {
      evg = new EpollEventLoopGroup(5);
    } else {
      evg = new NioEventLoopGroup(5);
    }
  }

  public void start() {
    try {
      if(!single_server) {
        zkClient.start();
        connectionPoolMgr.start();
        clientNodeWatcher.start();
        clientNodeWatcher.registerConnectionPoolManager(connectionPoolMgr);
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public void startAndWaitForNodes(int count){
    startAndWaitForNodes(count,5000);
  }

  public void startAndWaitForNodes(int count, long timeout) {
    if(!single_server) {
      try {
        latch = new CountDownLatch(count);
        start();
        latch.await(timeout,TimeUnit.MILLISECONDS);
        for (String node : buildNodeList()) {
          rendezvousHash.add(node);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void stop() throws Exception {
    log.info("ChicagoClient stopping");
    if(!single_server) {
      clientNodeWatcher.stop();
      connectionPoolMgr.stop();
      zkClient.stop();
    }
    evg.shutdownGracefully();
  }

  protected List<String> buildNodeList() {
    return zkClient.list(NODE_LIST_PATH);
  }

  public List<String> getNodeList(byte[] key) {
    return rendezvousHash.get(key);
  }

  public List<String> getEffectiveNodes(byte[] key){
    List<String> hashList = rendezvousHash.get(key);
    if(!single_server) {
      String path = REPLICATION_LOCK_PATH + "/" + new String(key);
      List<String> replicationList = clientNodeWatcher.getReplicationPathData(path);
      hashList.removeAll(replicationList);
    }
    return hashList;
  }
}
