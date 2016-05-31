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
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChicagoClient {
  private static final Logger log = LoggerFactory.getLogger(ChicagoClient.class);
  private final static String NODE_LIST_PATH = "/chicago/node-list";
  private static final long TIMEOUT = 1000;
  private static boolean TIMEOUT_ENABLED = true;
  private static int MAX_RETRY = 3;

  private final ExecutorService exe = Executors.newFixedThreadPool(20);

  private final InetSocketAddress single_server;
  private final RendezvousHash rendezvousHash;
  private final ClientNodeWatcher clientNodeWatcher;
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


  public ChicagoClient(InetSocketAddress server) {
    this.single_server = server;
    this.zkClient = null;
    this.quorum = 1;
    ArrayList<String> nodeList = new ArrayList<>();
    nodeList.add(server.getHostName());
    this.rendezvousHash = new RendezvousHash(Funnels.stringFunnel(Charset.defaultCharset()), nodeList, quorum);
    this.clientNodeWatcher = null;
    connectionPoolMgr = new ConnectionPoolManager(server.getHostName());
  }

  public ChicagoClient(String zkConnectionString, int quorum) throws InterruptedException {

    this.single_server = null;
    this.zkClient = new ZkClient(zkConnectionString);
    this.quorum = quorum;
    zkClient.start();

    this.rendezvousHash = new RendezvousHash(Funnels.stringFunnel(Charset.defaultCharset()), buildNodeList(), quorum);
    this.clientNodeWatcher = new ClientNodeWatcher(zkClient, rendezvousHash);
    this.connectionPoolMgr = new ConnectionPoolManager(zkClient);
  }

  public void start() {
    connectionPoolMgr.start();
  }

  public void stop() {
    log.info("ChicagoClient stopping");
    clientNodeWatcher.stop();
    connectionPoolMgr.stop();
    zkClient.stop();
  }

  protected List<String> buildNodeList() {
    return zkClient.list(NODE_LIST_PATH);
  }


  public ListenableFuture<byte[]> stream(byte[] key) throws ChicagoClientTimeoutException {
    return stream("chicago".getBytes(), key);
  }

  public ListenableFuture<byte[]> stream(byte[] colFam, byte[] key) throws ChicagoClientTimeoutException {
    ListeningExecutorService executor = MoreExecutors.listeningDecorator(exe);
    return executor.submit(() -> {
      ConcurrentLinkedDeque<Listener> listenerList = new ConcurrentLinkedDeque<>();
      ConcurrentLinkedDeque<UUID> idList = new ConcurrentLinkedDeque<>();
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
                  UUID id = UUID.randomUUID();
                  Listener listener = connectionPoolMgr.getListener(node); //Blocking
                  listenerList.add(listener);
                  cf.channel().writeAndFlush(new DefaultChicagoMessage(id, Op.READ, colFam, key, null));
                  listener.addID(id);
                  idList.add(id);
              });
            }
          }
        }

      } catch (ChicagoClientTimeoutException e) {
        Thread.currentThread().interrupt();
        log.error("Client Timeout During Read Operation:", e);
        return null;
      }


      while (idList.isEmpty()) {
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
      try {
        byte[] resp = (byte[]) listenerList.getFirst().getResponse(idList);
        if (resp != null) {
          return resp;
        }
      } catch (ChicagoClientTimeoutException e) {
        e.printStackTrace();
      }

      //TODO(JR): need to fix maybe?
      return read(colFam, key).get(TIMEOUT, TimeUnit.MILLISECONDS);

    });
  }

  public ListenableFuture<byte[]> read(byte[] key) throws ChicagoClientTimeoutException {
    return read("chicago".getBytes(), key);
  }

  public ListenableFuture<byte[]> read(byte[] colFam, byte[] key) throws ChicagoClientTimeoutException {
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
                  cf.channel().writeAndFlush(new DefaultChicagoMessage(id, Op.READ, colFam, key, null));
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

  public boolean write(byte[] key, byte[] value) throws ChicagoClientTimeoutException, ChicagoClientException {
    return write("chicago".getBytes(), key, value);
  }

  public boolean write(byte[] colFam, byte[] key, byte[] value) throws ChicagoClientTimeoutException, ChicagoClientException {
    long ts = System.currentTimeMillis();
    try {
      return _write(colFam, key, value, 0).get(TIMEOUT, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      long now = System.currentTimeMillis();
      log.error("TIMEOUT! " + (now - ts));
      throw new ChicagoClientTimeoutException(e);
    }

    return false;
  }


  private ListenableFuture<Boolean> _write(byte[] colFam, byte[] key, byte[] value, int _retries) throws ChicagoClientTimeoutException, ChicagoClientException {
    final int retries = _retries;
    ListeningExecutorService executor = MoreExecutors.listeningDecorator(exe);
    return executor.submit(() -> {

      ConcurrentLinkedDeque<Boolean> responseList = new ConcurrentLinkedDeque<>();
      final long startTime = System.currentTimeMillis();

      if (single_server != null) {
//      connect(single_server, Op.WRITE, key, value, listener);
      }

      try {

        List<String> hashList = rendezvousHash.get(key);

        for (String node : hashList) {
          if (node == null) {

          } else {
            log.debug(" +++++++++++++++++++++++++++++++++++++++++++++ Getting Node");
            ChannelFuture cf = connectionPoolMgr.getNode(node);
            log.debug(" +++++++++++++++++++++++++++++++++++++++++++++ Got Node");
            if (cf.channel().isWritable()) {
              exe.execute(() -> {
                  UUID id = UUID.randomUUID();
                  log.debug(" +++++++++++++++++++++++++++++++++++++++++++++ Getting Listener");
                  Listener listener = connectionPoolMgr.getListener(node); // Blocking
                  cf.channel().writeAndFlush(new DefaultChicagoMessage(id, Op.WRITE, colFam, key, value));
                  log.debug("++++++++++++++++++++++++++++++++++++++++ Write to node: " + node + " " + new String(key));
                  listener.addID(id);
                  exe.execute(() -> {
                      try {
                        log.debug(" ++++++++++++++++++++++++++++++++++++++ Getting Response for: " + new String(key) + " " + id);
                        responseList.add(listener.getStatus(id)); //Blocking
                        log.debug(" ======================================= Got Response for: " + new String(key) + " " + id);
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
        return false;
      }

      log.debug(" +++++++++++++++++++++++++++++++++++++++++++++++++++ Attempting to achive Quorum for key: " + new String(key));

      while (responseList.size() < quorum) {
        if (TIMEOUT_ENABLED && (System.currentTimeMillis() - startTime) > TIMEOUT) {
//            Thread.currentThread().interrupt();
          throw new ChicagoClientTimeoutException();
        }
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      log.debug(" +++++++++++++++++++++++++++++++++++++++++++++++++++ Returning response");
      if (responseList.stream().allMatch(b -> b)) {
        log.debug(" ========================================== Returned true ==================================================");
        return true;
      } else {
        if (MAX_RETRY < retries) {
          return _write(colFam, key, value, retries + 1).get(TIMEOUT, TimeUnit.MILLISECONDS);
        } else {
          _delete(colFam, key, 0);
          throw new ChicagoClientException("Could not successfully complete a replicated write. Please retry the operation");
        }
      }
    });
  }

  public boolean delete(byte[] key) throws ChicagoClientTimeoutException, ChicagoClientException {
    return delete("chicago".getBytes(), key);
  }

  public boolean delete(byte[] colFam, byte[] key) throws ChicagoClientTimeoutException, ChicagoClientException {
    try {
      return _delete(colFam, key, 0).get(TIMEOUT, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      throw new ChicagoClientTimeoutException();
    }

    return false;
  }

  private ListenableFuture<Boolean> _delete(byte[] colFam, byte[] key, int _retries) throws ChicagoClientTimeoutException, ChicagoClientException {
    final int retries = _retries;
    ListeningExecutorService executor = MoreExecutors.listeningDecorator(exe);
    return executor.submit(() -> {
      ConcurrentLinkedDeque<Boolean> responseList = new ConcurrentLinkedDeque<>();
      final long startTime = System.currentTimeMillis();

      try {

        List<String> hashList = rendezvousHash.get(key);

        for (String node : hashList) {
          if (node == null) {

          } else {
            ChannelFuture cf = connectionPoolMgr.getNode(node);
            if (cf.channel().isWritable()) {
              exe.execute(() -> {
                  UUID id = UUID.randomUUID();
                  Listener listener = connectionPoolMgr.getListener(node);
                  cf.channel().writeAndFlush(new DefaultChicagoMessage(id, Op.DELETE, colFam, key, null));
                  listener.addID(id);
                  exe.execute(() -> {
                      try {
                        responseList.add(listener.getStatus(id));
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
        log.error("Client Timeout During Delete Operation: ", e);
        return false;
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


      if (responseList.stream().allMatch(b -> b)) {
        return true;
      } else {
        if (MAX_RETRY < retries) {
          return _delete(colFam, key, retries + 1).get(TIMEOUT, TimeUnit.MILLISECONDS);
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
