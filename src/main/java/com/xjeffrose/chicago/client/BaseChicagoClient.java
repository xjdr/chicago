package com.xjeffrose.chicago.client;

import com.google.common.hash.Funnels;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.xjeffrose.chicago.DefaultChicagoMessage;
import com.xjeffrose.chicago.Op;
import com.xjeffrose.chicago.ZkClient;
import io.netty.channel.ChannelFuture;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for Chicago clients
 */
public class BaseChicagoClient {
  private static final Logger log = LoggerFactory.getLogger(BaseChicagoClient.class);
  protected final static String NODE_LIST_PATH = "/chicago/node-list";
  protected static final long TIMEOUT = 1000;
  protected static boolean TIMEOUT_ENABLED = true;
  protected static int MAX_RETRY = 3;
  protected final AtomicInteger nodesAvailable = new AtomicInteger(0);

  protected final ExecutorService exe = Executors.newFixedThreadPool(20);

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

  public BaseChicagoClient(String address){
    this.single_server = true;
    this.zkClient = null;
    this.quorum = 1;
    ArrayList<String> nodeList = new ArrayList<>();
    nodeList.add(address);
    this.rendezvousHash =  new RendezvousHash(Funnels.stringFunnel(Charset.defaultCharset()), nodeList, quorum);
    clientNodeWatcher = null;
    this.connectionPoolMgr = new ConnectionPoolManager(address);
  }

  public BaseChicagoClient(String zkConnectionString, int quorum) throws InterruptedException {

    this.single_server = false;
    this.zkClient = new ZkClient(zkConnectionString);
    this.quorum = quorum;

    ArrayList<String> nodeList = new ArrayList<>();
    this.rendezvousHash = new RendezvousHash(Funnels.stringFunnel(Charset.defaultCharset()), nodeList, quorum);
    clientNodeWatcher = new ClientNodeWatcher(zkClient, rendezvousHash, listener);
    this.connectionPoolMgr = new ConnectionPoolManager(zkClient);
  }

  public void start() {
    try {
      if(!single_server) {
        zkClient.start();
        connectionPoolMgr.start();
        clientNodeWatcher.start();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void startAndWaitForNodes(int count) {
    if(!single_server) {
      try {
        latch = new CountDownLatch(count);
        start();
        latch.await();
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
  }

  protected List<String> buildNodeList() {
    return zkClient.list(NODE_LIST_PATH);
  }

  public List<String> getNodeList(byte[] key) {
    return rendezvousHash.get(key);
  }
}
