package com.xjeffrose.chicago.client;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.xjeffrose.chicago.DefaultChicagoMessage;
import com.xjeffrose.chicago.Op;
import io.netty.channel.ChannelFuture;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChicagoClient extends BaseChicagoClient {
  public class WriteState {
    public int attempt;
    public ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();

    WriteState() {
      attempt = 1;
    }

    public void nodeState(String node, String state) {
      map.put(node, state);
    }

    public String toString() {
      return map.toString();
    }
  }
  public WriteState writeState;
  private static final Logger log = LoggerFactory.getLogger(ChicagoClient.class);

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

  public ChicagoClient(String zkConnectionString, int quorum) throws InterruptedException {
    super(zkConnectionString, quorum);
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
      if (TIMEOUT_ENABLED) {
        return read(colFam, key).get(TIMEOUT, TimeUnit.MILLISECONDS);
      } else {
        return read(colFam, key).get();
      }

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
      if (TIMEOUT_ENABLED) {
        return _write(colFam, key, value, 0).get(TIMEOUT, TimeUnit.MILLISECONDS);
      } else {
        return _write(colFam, key, value, 0).get();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      long now = System.currentTimeMillis();
      log.error("TIMEOUT! " + (now - ts));
      log.error("Write State: " + writeState);
      throw new ChicagoClientTimeoutException(e);
    }

    return false;
  }


  private ListenableFuture<Boolean> _write(byte[] colFam, byte[] key, byte[] value, int _retries) throws ChicagoClientTimeoutException, ChicagoClientException {
    final int retries = _retries;
    ListeningExecutorService executor = MoreExecutors.listeningDecorator(exe);
    writeState = new WriteState();
    return executor.submit(() -> {

      ConcurrentLinkedDeque<Boolean> responseList = new ConcurrentLinkedDeque<>();
      final long startTime = System.currentTimeMillis();
      try {

        List<String> hashList = rendezvousHash.get(key);

        for (String node : hashList) {
          if (node == null) {

          } else {
            log.debug(" +++++++++++++++++++++++++++++++++++++++++++++ Getting Node");
            ChannelFuture cf = connectionPoolMgr.getNode(node);
            log.debug(" +++++++++++++++++++++++++++++++++++++++++++++ Got Node");
            if (cf.channel().isWritable()) {
              writeState.nodeState(node, "dispatch");
              exe.execute(() -> {
                  UUID id = UUID.randomUUID();
                  log.debug(" +++++++++++++++++++++++++++++++++++++++++++++ Getting Listener");
                  Listener listener = connectionPoolMgr.getListener(node); // Blocking
                  writeState.nodeState(node, "write");
                  cf.channel().writeAndFlush(new DefaultChicagoMessage(id, Op.WRITE, colFam, key, value));
                  writeState.nodeState(node, "finished writing");
                  log.debug("++++++++++++++++++++++++++++++++++++++++ Write to node: " + node + " " + new String(key));
                  listener.addID(id);
                  exe.execute(() -> {
                      try {
                        log.debug(" ++++++++++++++++++++++++++++++++++++++ Getting Response for: " + new String(key) + " " + id);
                        writeState.nodeState(node, "waiting for read");
                        responseList.add(listener.getStatus(id)); //Blocking
                        writeState.nodeState(node, "finished reading");
                        log.debug(" ======================================= Got Response for: " + new String(key) + " " + id);
                      } catch (ChicagoClientTimeoutException e) {
//                          Thread.currentThread().interrupt();
                        writeState.nodeState(node, "read timeout");
                        throw new RuntimeException(e);
                      }
                  });
              });
            } else {
              log.error("Channel was not writable");
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
          log.error("Quorum timeout");
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
          log.error("write failed, retrying(" + retries + ")");
          if (TIMEOUT_ENABLED) {
            return _write(colFam, key, value, retries + 1).get(TIMEOUT, TimeUnit.MILLISECONDS);
          } else {
            return _write(colFam, key, value, retries + 1).get();
          }
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
      if (TIMEOUT_ENABLED) {
        return _delete(colFam, key, 0).get(TIMEOUT, TimeUnit.MILLISECONDS);
      } else {
        return _delete(colFam, key, 0).get();
      }
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
          if (TIMEOUT_ENABLED) {
            return _delete(colFam, key, retries + 1).get(TIMEOUT, TimeUnit.MILLISECONDS);
          } else {
            return _delete(colFam, key, retries + 1).get();
          }
        } else {
          throw new ChicagoClientException("Could not successfully complete a replicated write. Please retry the operation");
        }
      }
    });
  }
}
