package com.xjeffrose.chicago.client;

import com.google.common.hash.Funnels;
import com.google.common.util.concurrent.SettableFuture;
import com.xjeffrose.chicago.NodeListener;
import com.xjeffrose.chicago.RendezvousHash;
import com.xjeffrose.chicago.ZkClient;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.internal.PlatformDependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract base class for Chicago clients
 */
abstract public class BaseChicagoClient {
  private static final Logger log = LoggerFactory.getLogger(BaseChicagoClient.class);
  protected final static String NODE_LIST_PATH = "/chicago/node-list";
  public final static String REPLICATION_LOCK_PATH ="/chicago/replication-lock";
  protected static final long TIMEOUT = 3000;
  protected static boolean TIMEOUT_ENABLED = true;
  protected static int MAX_RETRY = 3;
  protected final AtomicInteger nodesAvailable = new AtomicInteger(0);
  protected static final Map<String, Long> lastOffsetMap = PlatformDependent.newConcurrentHashMap();
  protected final boolean single_server;
  protected final RendezvousHash<String> rendezvousHash;
  protected final ClientNodeWatcher clientNodeWatcher;
  protected final ChicagoBuffer chicagoBuffer;
  private CountDownLatch latch;
  private final NodeListener listener = new NodeListener() {
    @Override
    public void nodeAdded(Object node) {
      if (latch != null) {
        latch.countDown();
      }
    }

    @Override
    public void nodeRemoved(Object node) {
      nodesAvailable.decrementAndGet();
    }
  };
  protected final ZkClient zkClient;
  protected final ConnectionPoolManagerX connectionPoolMgr;
  protected int quorum;
  protected Map<UUID, SettableFuture<byte[]>> futureMap;
  protected EventLoopGroup evg;


  public BaseChicagoClient(String address) throws InterruptedException{
    this.single_server = true;
    // this is only for test purpose. The zkServer should run on the same machine as that of the chicago server.
    //this.zkClient = new ZkClient(address.split(":")[0]+":2181", false);
    //this.zkClient.start();
    this.zkClient = null;
    this.quorum = 1;
    ArrayList<String> nodeList = new ArrayList<>();
    nodeList.add(address);
    this.rendezvousHash =  new RendezvousHash(Funnels.stringFunnel(Charset.defaultCharset()), nodeList, quorum);
    // only if the zkserver runs in the same machine as the chicago server [Test purpose]
    //clientNodeWatcher = new ClientNodeWatcher(zkClient, rendezvousHash, listener);
    clientNodeWatcher = null;
    this.futureMap = PlatformDependent.newConcurrentHashMap();
    this.connectionPoolMgr = new ConnectionPoolManagerX(address, futureMap);
    this.chicagoBuffer = new ChicagoBuffer(connectionPoolMgr, this);
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
    this.futureMap = PlatformDependent.newConcurrentHashMap();

    ArrayList<String> nodeList = new ArrayList<>();
    this.rendezvousHash = new RendezvousHash(Funnels.stringFunnel(Charset.defaultCharset()), nodeList, quorum);
    clientNodeWatcher = new ClientNodeWatcher(zkClient);
    this.connectionPoolMgr = new ConnectionPoolManagerX(zkClient, futureMap);
    this.chicagoBuffer = new ChicagoBuffer(connectionPoolMgr, this);
    if (Epoll.isAvailable()) {
      evg = new EpollEventLoopGroup(5);
    } else {
      evg = new NioEventLoopGroup(5);
    }
  }

  BaseChicagoClient(List<EmbeddedChannel> hostPool, Map<UUID, SettableFuture<byte[]>> futureMap, int quorum) throws InterruptedException {

    this.single_server = false;
    this.zkClient = null;
    this.quorum = quorum;
    this.futureMap = futureMap;

    ArrayList<String> nodeList = new ArrayList<>();
    hostPool.stream().forEach(xs -> nodeList.add(xs.remoteAddress().toString()));
    this.rendezvousHash = new RendezvousHash(Funnels.stringFunnel(Charset.defaultCharset()), nodeList, quorum);
    clientNodeWatcher = null;
    this.connectionPoolMgr = new ConnectionPoolManagerX(hostPool, futureMap);
    this.chicagoBuffer = new ChicagoBuffer(connectionPoolMgr, this);
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
        clientNodeWatcher.registerListener(rendezvousHash);
        clientNodeWatcher.registerListener(listener);
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
    try {
      if(!single_server) {
        latch = new CountDownLatch(count);
        start();
        latch.await(timeout, TimeUnit.MILLISECONDS);
        for (String node : buildNodeList()) {
          rendezvousHash.add(node);
        }
      }
      while(connectionPoolMgr.getConnectionMapSize() < count){
        Thread.sleep(0,500);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void stop() throws Exception {
    log.info("ChicagoClient stopping");
    if(!single_server && !(clientNodeWatcher == null)) {
      clientNodeWatcher.stop();
      connectionPoolMgr.stop();
      if (zkClient != null) {
        zkClient.stop();
      }
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
    List<String> hashList = new ArrayList<>(rendezvousHash.get(key));
    if(!single_server && !(clientNodeWatcher == null)) {
      String path = REPLICATION_LOCK_PATH + "/" + new String(key);
      List<String> replicationList = clientNodeWatcher.getReplicationPathData(path);
      hashList.removeAll(replicationList);
    }

    return hashList;
  }

  public List<String> getAllColFamily() throws Exception {
    List<String> resp = new ArrayList<>();
    if(this.zkClient != null) {
      resp = this.zkClient.list(REPLICATION_LOCK_PATH);
      return resp;
    } else {
      return resp;
    }
  }
}
