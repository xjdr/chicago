package com.xjeffrose.chicago.client;

import com.google.common.hash.Funnels;
import com.google.common.primitives.Booleans;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.xjeffrose.chicago.Chicago;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChicagoTSClient {
  private static final Logger log = LoggerFactory.getLogger(ChicagoTSClient.class);
  private final static String NODE_LIST_PATH = "/chicago/node-list";
  private static final long TIMEOUT = 10000000;
  private static boolean TIMEOUT_ENABLED = false;
  private static int MAX_RETRY = 3;

  private final ExecutorService exe = Executors.newFixedThreadPool(20);

  private final InetSocketAddress single_server;
  private final RendezvousHash rendezvousHash;
  private final ClientNodeWatcher clientNodeWatcher;
  private final AtomicInteger nodesAvailable = new AtomicInteger(0);
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
  private final ZkClient zkClient;
  private final ConnectionPoolManager connectionPoolMgr;
  private final int quorum;
    /*
   * Happy Path:
   * Delete -> send message to all (3) available nodes wait for all (3) responses to be true.
   * Write -> send message to all (3) available nodes wait for all (3) responses to be true.
   * Read -> send message to all (3) available nodes, wait for 1 node to reply, all other (2) replies are dropped.
   *
   * Fail Path:
   * Delete -> not all responses are true
   * Write -> not all responses are true
   * Read -> no nodes respond
   *
   * Reading from a node that hasn't been able to receive writes
   * Write fails, some nodes think that they have good data until they're told that they don't
   * interleaved writes from two different clients for the same key
   *
   *
   *
   *
   * two phase commit with multiple nodes
   *  write (key, value)
   *  ack x 3 nodes
   *  ok x 3 nodes -> write request
   */

  public ChicagoTSClient(InetSocketAddress server) {
    this.single_server = server;
    this.zkClient = null;
    this.quorum = 1;
    ArrayList<String> nodeList = new ArrayList<>();
    nodeList.add(server.getHostName());
    this.rendezvousHash = new RendezvousHash(Funnels.stringFunnel(Charset.defaultCharset()), nodeList, quorum);
    this.clientNodeWatcher = null;
    connectionPoolMgr = new ConnectionPoolManager(server.getHostName());
  }

  public ChicagoTSClient(String zkConnectionString, int quorum) throws InterruptedException {

    this.single_server = null;
    this.zkClient = new ZkClient(zkConnectionString);
    this.quorum = quorum;

    ArrayList<String> nodeList = new ArrayList<>();
    this.rendezvousHash = new RendezvousHash(Funnels.stringFunnel(Charset.defaultCharset()), nodeList, quorum);
    this.clientNodeWatcher = new ClientNodeWatcher(zkClient, rendezvousHash, listener);
    this.connectionPoolMgr = new ConnectionPoolManager(zkClient);
  }

  public void start() {
    try {
      zkClient.start();
      connectionPoolMgr.start();
      clientNodeWatcher.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void startAndWaitForNodes(int count) {
    try {
      latch = new CountDownLatch(count);
      start();
      latch.await();
      for (String node: buildNodeList()) {
        rendezvousHash.add(node);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void stop() {
    try {
    zkClient.stop();
    connectionPoolMgr.stop();
    clientNodeWatcher.stop();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  protected List<String> buildNodeList() {
    return zkClient.list(NODE_LIST_PATH);
  }

  public ListenableFuture<ChicagoStream> stream(byte[] key) throws ChicagoClientTimeoutException {
    return stream(key, null);
  }

  public ListenableFuture<ChicagoStream> stream(byte[] key, byte[] offset) throws ChicagoClientTimeoutException {
    ListeningExecutorService executor = MoreExecutors.listeningDecorator(exe);
    return executor.submit(() -> {
      final ChicagoStream[] cs = new ChicagoStream[1];
        final long startTime = System.currentTimeMillis();
        if (single_server != null) {
        }
        try {
          List<String> hashList = rendezvousHash.get(key);
          for (String node : hashList) {
            if (node == null) {
            } else {
              ChannelFuture cf = connectionPoolMgr.getNode(node);
              if (cf.channel().isWritable()) {
                exe.execute(() -> {
//                try {
                    UUID id = UUID.randomUUID();
                    Listener listener = connectionPoolMgr.getListener(node); //Blocking
                    cs[0] = new ChicagoStream(listener);
                    cf.channel().writeAndFlush(new DefaultChicagoMessage(id, Op.STREAM, key, null, offset));
                    listener.addID(id);
                    cs[0].addID(id);
                });
              }
            }
          }

        } catch (ChicagoClientTimeoutException e) {
          Thread.currentThread().interrupt();
          log.error("Client Timeout During Read Operation:", e);
          return null;
        }

        while (cs[0] == null) {
          if (TIMEOUT_ENABLED && (System.currentTimeMillis() - startTime) > TIMEOUT) {
            Thread.currentThread().interrupt();
            throw new ChicagoClientTimeoutException();
          }
          try {
            Thread.sleep(1);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }

        return cs[0];
//      }

    });

  }

  public ListenableFuture<byte[]> read(byte[] key) throws ChicagoClientTimeoutException {
    return read("chicago".getBytes(), key);
  }

  public ListenableFuture<byte[]> read(byte[] key, byte[] offset) throws ChicagoClientTimeoutException {
    ListeningExecutorService executor = MoreExecutors.listeningDecorator(exe);
    return executor.submit(() -> {
      final long startTime = System.currentTimeMillis();
      ConcurrentLinkedDeque<byte[]> responseList = new ConcurrentLinkedDeque<>();
      if (single_server != null) {
      }
      try {
        List<String> hashList = rendezvousHash.get(key);
        for (String node : hashList) {
          if (node == null) {
          } else {
            ChannelFuture cf = connectionPoolMgr.getNode(node);
            if (cf.channel().isWritable()) {
              exe.execute(() -> {
                UUID id = UUID.randomUUID();
                Listener listener = connectionPoolMgr.getListener(node); //Blocking
                cf.channel().writeAndFlush(new DefaultChicagoMessage(id, Op.STREAM, key, null, offset));
                listener.addID(id);
                exe.execute(() -> {
                  try {
                    responseList.add((byte[]) listener.getResponse(id)); //Blocking
                  } catch (ChicagoClientTimeoutException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                  }
                });
              });
            }
          }
        }

      } catch (ChicagoClientTimeoutException e) {
        Thread.currentThread().interrupt();
        log.error("Client Timeout During Read Operation:", e);
        return null;
      }


      while (responseList.isEmpty()) {
        if (TIMEOUT_ENABLED && (System.currentTimeMillis() - startTime) > TIMEOUT) {
          Thread.currentThread().interrupt();
          throw new ChicagoClientTimeoutException();
        }
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      return responseList.stream().findFirst().orElse(null);
    });
  }

  public byte[] write(byte[] key, byte[] value) throws ChicagoClientTimeoutException, ChicagoClientException {
    try {
      if (TIMEOUT_ENABLED) {
        return _write(key, value, 0).get(TIMEOUT, TimeUnit.MILLISECONDS);
      } else {
        return _write(key, value, 0).get();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      throw new ChicagoClientTimeoutException();
    }

    return null;
  }


  private ListenableFuture<byte[]> _write(byte[] key, byte[] value, int _retries) throws ChicagoClientTimeoutException, ChicagoClientException {
    final int retries = _retries;


    final ConcurrentLinkedDeque<byte[]> responseList = new ConcurrentLinkedDeque<>();
    final ConcurrentLinkedDeque<UUID> idList = new ConcurrentLinkedDeque<>();
    final ConcurrentLinkedDeque<Listener> listenerList = new ConcurrentLinkedDeque<>();

    ListeningExecutorService executor = MoreExecutors.listeningDecorator(exe);
    return executor.submit(() -> {

        final long startTime = System.currentTimeMillis();

        if (single_server != null) {
//      connect(single_server, Op.WRITE, key, value, listener);
        }

        try {

          List<String> hashList = rendezvousHash.get(key);

          for (String node : hashList) {
            if (node == null) {

            } else {
              ChannelFuture cf = connectionPoolMgr.getNode(node);
              if (cf.channel().isWritable()) {
                exe.execute(() -> {
                    UUID id = UUID.randomUUID();
                    Listener listener = connectionPoolMgr.getListener(node); // Blocking
                    cf.channel().writeAndFlush(new DefaultChicagoMessage(id, Op.TS_WRITE, key, null, value));
                    listener.addID(id);
                    idList.add(id);
                    listenerList.add(listener);
                    exe.execute(() -> {
                        try {
                          responseList.add((byte[]) listener.getStatus(idList)); //Blocking
                        } catch (ChicagoClientTimeoutException e) {
//                          Thread.currentThread().interrupt();
                          throw new RuntimeException(e);
                        }
                    });
                });
              }
            }
          }

        } catch (ChicagoClientTimeoutException e) {
          log.error("Client Timeout During Write Operation: ", e);
          return null;
        }


        while (responseList.size() < quorum) {
          if (TIMEOUT_ENABLED && (System.currentTimeMillis() - startTime) > TIMEOUT) {
            Thread.currentThread().interrupt();
            throw new ChicagoClientTimeoutException();
          }
          try {
            Thread.sleep(1);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }

        if (responseList.stream().allMatch(b -> b != null)) {
//          return (byte[]) listenerList.removeFirst().getResponse(idList.getFirst());
          return responseList.getFirst();
        } else {
          if (MAX_RETRY < retries) {
            return _write(key, value, retries + 1).get(TIMEOUT, TimeUnit.MILLISECONDS);
          } else {
            throw new ChicagoClientException("Could not successfully complete a replicated write. Please retry the operation");
          }
        }
    });
  }


  public List<String> getNodeList(byte[] key) {
    return rendezvousHash.get(key);
  }
}
