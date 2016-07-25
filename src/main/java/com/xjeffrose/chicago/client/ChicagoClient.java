package com.xjeffrose.chicago.client;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.xjeffrose.chicago.ChiUtil;
import com.xjeffrose.chicago.ChicagoMessage;
import com.xjeffrose.chicago.DefaultChicagoMessage;
import com.xjeffrose.chicago.Op;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
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

  public ChicagoClient(String address) throws InterruptedException {
    super(address);
  }

  public ListenableFuture<byte[]> read(byte[] key) throws ChicagoClientTimeoutException {
    return read(ChiUtil.defaultColFam.getBytes(), key);
  }

  public ListenableFuture<byte[]> read(byte[] colFam, byte[] key) throws ChicagoClientTimeoutException {
    ListeningExecutorService executor = MoreExecutors.listeningDecorator(exe);
    CountDownLatch latch = new CountDownLatch(1);
    return executor.submit(() -> {
      final long startTime = System.currentTimeMillis();
      ConcurrentLinkedDeque<byte[]> responseList = new ConcurrentLinkedDeque<>();
      List<String> hashList = getEffectiveNodes(key);
      for (String node : hashList) {
        if (node == null) {
        } else {
          ChannelFuture cf = connectionPoolMgr.getNode(node);
          if (cf.channel().isWritable()) {
            exe.execute(() -> {
              UUID id = UUID.randomUUID();
              Listener listener = connectionPoolMgr.getListener(node);
              cf.channel().writeAndFlush(new DefaultChicagoMessage(id, Op.READ, colFam, key, null));
              listener.addID(id);
              listener.registerCallback(id, new ResponseListener() {
                @Override public void onResponse(ChicagoMessage chicagoMessage, ChannelHandlerContext ctx) {
                  if(latch.getCount() > 0) {
                    log.debug("Got response from context " + ctx.channel());
                  }
                  if(responseList.isEmpty()) {
                    responseList.add(chicagoMessage.getVal());
                  }
                  latch.countDown();
                }
              });
            });
          }
        }
        if(latch.getCount() > 0) {
          Thread.sleep(5);
        }
      }

      try{
        //Wait for response from anyone server.
        latch.await(TIMEOUT, TimeUnit.MILLISECONDS);
      } catch ( InterruptedException e){
        log.error("Failed to get response from any server !!!");
        Thread.currentThread().interrupt();
        throw new ChicagoClientTimeoutException();
      }

      return responseList.stream().findFirst().orElse(null);
    });
  }

  public boolean write(byte[] key, byte[] value) throws ChicagoClientTimeoutException, ChicagoClientException {
    return write(ChiUtil.defaultColFam.getBytes(), key, value);
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

    final ConcurrentLinkedDeque<byte[]> responseList = new ConcurrentLinkedDeque<>();
    final ConcurrentLinkedDeque<UUID> idList = new ConcurrentLinkedDeque<>();

    ListeningExecutorService executor = MoreExecutors.listeningDecorator(exe);
    return executor.submit(() -> {
      int dquorum = quorum;
      final long startTime = System.currentTimeMillis();
      List<String> hashList = getEffectiveNodes(key);
      dquorum = hashList.size();
      CountDownLatch latch = new CountDownLatch(dquorum);
      log.debug("Writing to "+hashList.toString());
      hashList.stream().parallel().forEach(node -> {
        if (node == null) {

        } else {
          try {
            ChannelFuture cf = connectionPoolMgr.getNode(node);
            if (cf.channel().isWritable()) {
              exe.execute(() -> {
                UUID id = UUID.randomUUID();
                Listener listener = connectionPoolMgr.getListener(node); // Blocking
                cf.channel().writeAndFlush(new DefaultChicagoMessage(id, Op.WRITE, colFam, key, value));
                listener.addID(id);
                idList.add(id);
                listener.registerCallback(id, new ResponseListener() {
                  @Override public void onResponse(ChicagoMessage chicagoMessage, ChannelHandlerContext ctx) {
                    responseList.add(chicagoMessage.getVal());
                    latch.countDown();
                  }
                });
              });
            }
          } catch (ChicagoClientTimeoutException e){
            log.error("Client Timeout During Write Operation: ", e);
          }
        }
      });

      try {
        //Wait for all the servers to respond
        latch.await(TIMEOUT, TimeUnit.MILLISECONDS);
      }catch (InterruptedException e){
        log.error("Write failed. Expected number of response :"+dquorum +
          ". Number of response received :"+responseList.size());
        Thread.currentThread().interrupt();
        throw new ChicagoClientTimeoutException();
      }

      if (responseList.stream().allMatch(b -> b != null)) {
        return true;
      } else {
        System.out.println("Return value from different servers are not same. Trying to write again!!!");
        if (MAX_RETRY < retries) {
          if (TIMEOUT_ENABLED) {
            return _write(colFam, key, value, retries + 1).get(TIMEOUT, TimeUnit.MILLISECONDS);
          } else {
            return _write(colFam, key, value, retries + 1).get();
          }
        } else {
          throw new ChicagoClientException("Could not successfully complete a replicated write. Please retry the operation");
        }
      }
    });
  }

  //Todo: Delete might not work properly during replication because we are not handling the Effective Node list
  public boolean delete(byte[] key) throws ChicagoClientTimeoutException, ChicagoClientException {
    return delete(ChiUtil.defaultColFam.getBytes(), key);
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

  public  ListenableFuture<Boolean> deleteColFam(byte[] colFam) throws ChicagoClientTimeoutException, ChicagoClientException {
    final int retries = 0;
    ListeningExecutorService executor = MoreExecutors.listeningDecorator(exe);
    return executor.submit(() -> {
      ConcurrentLinkedDeque<Boolean> responseList = new ConcurrentLinkedDeque<>();
      final long startTime = System.currentTimeMillis();

      try {

        List<String> hashList = rendezvousHash.get(colFam);
        for (String node : hashList) {
          if (node == null) {

          } else {
            ChannelFuture cf = connectionPoolMgr.getNode(node);
            if (cf.channel().isWritable()) {
              exe.execute(() -> {
                UUID id = UUID.randomUUID();
                Listener listener = connectionPoolMgr.getListener(node);
                cf.channel().writeAndFlush(new DefaultChicagoMessage(id, Op.DELETE, colFam, null, null));
                listener.addID(id);
                listener.registerCallback(id, new ResponseListener() {
                  @Override public void onResponse(ChicagoMessage chicagoMessage, ChannelHandlerContext ctx) {
                    responseList.add(chicagoMessage.getSuccess());
                  }
                });

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
            return _delete(colFam, null, retries + 1).get(TIMEOUT, TimeUnit.MILLISECONDS);
          } else {
            return _delete(colFam, null, retries + 1).get();
          }
        } else {
          throw new ChicagoClientException("Could not successfully complete a replicated write. Please retry the operation");
        }
      }
    });
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
