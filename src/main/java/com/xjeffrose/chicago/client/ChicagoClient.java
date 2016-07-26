package com.xjeffrose.chicago.client;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.xjeffrose.chicago.DefaultChicagoMessage;
import com.xjeffrose.chicago.Op;
import io.netty.channel.ChannelFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChicagoClient extends BaseChicagoClient {
  private static final Logger log = LoggerFactory.getLogger(ChicagoClient.class);
  public WriteState writeState;
  public ChicagoClient(String zkConnectionString, int quorum) throws InterruptedException {
    super(zkConnectionString, quorum);
  }

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

  public ChicagoClient(String address) throws InterruptedException {
    super(address);
  }

  public ListenableFuture<List<byte[]>> stream(byte[] key) throws ChicagoClientTimeoutException {
    return stream(key, null);
  }

  public ListenableFuture<List<byte[]>> stream(byte[] key,  byte[] offset) throws ChicagoClientTimeoutException {
    ConcurrentLinkedDeque<byte[]> responseList = new ConcurrentLinkedDeque<>();
    List<ListenableFuture<byte[]>> relevantFutures = new ArrayList<>();
    List<String> hashList = rendezvousHash.get(key);
    String node = hashList.get(0);
    if (node == null) {
    } else {
      ChannelFuture cf = connectionPoolMgr.getNode(node);
      if (cf.channel().isWritable()) {
        UUID id = UUID.randomUUID();
        SettableFuture<byte[]> f = SettableFuture.create();
        Futures.addCallback(f, new FutureCallback<byte[]>() {
          @Override
          public void onSuccess(@Nullable byte[] bytes) {
            if (relevantFutures.size() > 1) {
              relevantFutures.get(1).cancel(true);
            }          }

          @Override
          public void onFailure(Throwable throwable) {

          }
        });
        futureMap.put(id, f);
        relevantFutures.add(f);
        cf.channel().writeAndFlush(new DefaultChicagoMessage(id, Op.STREAM, key, null, offset));

        evg.schedule(() -> {
          String node1 = hashList.get(1);
          if (node1 == null) {
          } else {
            ChannelFuture cf1 = null;
            try {
              cf1 = connectionPoolMgr.getNode(node1);
            } catch (ChicagoClientTimeoutException e) {
              e.printStackTrace();
            }
            if (cf1.channel().isWritable()) {
              UUID id1 = UUID.randomUUID();
              SettableFuture<byte[]> f1 = SettableFuture.create();
              Futures.addCallback(f1, new FutureCallback<byte[]>() {
                @Override
                public void onSuccess(@Nullable byte[] bytes) {

                }

                @Override
                public void onFailure(Throwable throwable) {

                }
              });
              futureMap.put(id1, f1);
              relevantFutures.add(f1);
              cf.channel().writeAndFlush(new DefaultChicagoMessage(id1, Op.STREAM, key, null, offset));
            }
          }
        }, 2, TimeUnit.MILLISECONDS);

        return Futures.successfulAsList(relevantFutures);
      }
    }
    return null;
  }

  public ListenableFuture<List<byte[]>> read(byte[] key) throws ChicagoClientTimeoutException {
    return read("chicago".getBytes(), key);
  }

  public ListenableFuture<List<byte[]>> read(byte[] colFam, byte[] key) throws ChicagoClientTimeoutException {
    ConcurrentLinkedDeque<byte[]> responseList = new ConcurrentLinkedDeque<>();
    List<ListenableFuture<byte[]>> relevantFutures = new ArrayList<>();
    List<String> hashList = rendezvousHash.get(key);
    String node = hashList.get(0);
    if (node == null) {
    } else {
      ChannelFuture cf = connectionPoolMgr.getNode(node);
      if (cf.channel().isWritable()) {
        UUID id = UUID.randomUUID();
        SettableFuture<byte[]> f = SettableFuture.create();
        Futures.addCallback(f, new FutureCallback<byte[]>() {
          @Override
          public void onSuccess(@Nullable byte[] bytes) {
            if (relevantFutures.size() > 1) {
              relevantFutures.get(1).cancel(true);
            }
          }

          @Override
          public void onFailure(Throwable throwable) {

          }
        });
        futureMap.put(id, f);
        relevantFutures.add(f);
        cf.channel().writeAndFlush(new DefaultChicagoMessage(id, Op.READ, colFam, key, null));

        evg.schedule(() -> {
          String node1 = hashList.get(1);
          if (node1 == null) {
          } else {
            ChannelFuture cf1 = null;
            try {
              cf1 = connectionPoolMgr.getNode(node1);
            } catch (ChicagoClientTimeoutException e) {
              e.printStackTrace();
            }
            if (cf1.channel().isWritable()) {
              UUID id1 = UUID.randomUUID();
              SettableFuture<byte[]> f1 = SettableFuture.create();
              Futures.addCallback(f1, new FutureCallback<byte[]>() {
                @Override
                public void onSuccess(@Nullable byte[] bytes) {
                }

                @Override
                public void onFailure(Throwable throwable) {

                }
              });
              futureMap.put(id1, f1);
              relevantFutures.add(f1);
              cf.channel().writeAndFlush(new DefaultChicagoMessage(id1, Op.READ, colFam, key, null));
            }
          }
        }, 2, TimeUnit.MILLISECONDS);

        return Futures.successfulAsList(relevantFutures);
      }
    }
    return null;
  }

  public ListenableFuture<List<byte[]>> write(byte[] key, byte[] value) throws ChicagoClientTimeoutException, ChicagoClientException {
    return write("chicago".getBytes(), key, value);
  }

  public ListenableFuture<List<byte[]>> write(byte[] colFam, byte[] key, byte[] value) throws ChicagoClientTimeoutException, ChicagoClientException {
    return _write(colFam, key, value, 0);
  }

  private ListenableFuture<List<byte[]>> _write(byte[] colFam, byte[] key, byte[] value, int _retries) throws ChicagoClientTimeoutException, ChicagoClientException {
    final int retries = _retries;
    List<ListenableFuture<byte[]>> relevantFutures = new ArrayList<>();

    final long startTime = System.currentTimeMillis();

        List<String> hashList = rendezvousHash.get(key);

        for (String node : hashList) {
          if (node == null) {
          } else {
            ChannelFuture cf = connectionPoolMgr.getNode(node);
            if (cf.channel().isWritable()) {
              UUID id = UUID.randomUUID();
              SettableFuture<byte[]> f = SettableFuture.create();
              Futures.addCallback(f, new FutureCallback<byte[]>() {
                @Override
                public void onSuccess(@Nullable byte[] bytes) {

                }

                @Override
                public void onFailure(Throwable throwable) {

                }
              });
              futureMap.put(id, f);
              relevantFutures.add(f);
                cf.channel().writeAndFlush(new DefaultChicagoMessage(id, Op.WRITE, colFam, key, value));
          }
        }
      }
    return Futures.successfulAsList(relevantFutures);
  }

  public ListenableFuture<List<byte[]>> tsWrite(byte[] key, byte[] value) throws ChicagoClientTimeoutException, ChicagoClientException {
    return _tsWrite(null, key, value, 0);
  }

  public ListenableFuture<List<byte[]>> tsWrite(byte[] colFam, byte[] key, byte[] value) throws ChicagoClientTimeoutException, ChicagoClientException {
    return _tsWrite(colFam, key, value, 0);
  }

  private ListenableFuture<List<byte[]>> _tsWrite(byte[] colFam, byte[] key, byte[] value, int _retries) throws ChicagoClientTimeoutException, ChicagoClientException {
    final int retries = _retries;
    List<ListenableFuture<byte[]>> relevantFutures = new ArrayList<>();

    final long startTime = System.currentTimeMillis();

    List<String> hashList = rendezvousHash.get(key);

    for (String node : hashList) {
      if (node == null) {
      } else {
        ChannelFuture cf = connectionPoolMgr.getNode(node);
        if (cf.channel().isWritable()) {
          UUID id = UUID.randomUUID();
          SettableFuture<byte[]> f = SettableFuture.create();
          Futures.addCallback(f, new FutureCallback<byte[]>() {
            @Override
            public void onSuccess(@Nullable byte[] bytes) {

            }

            @Override
            public void onFailure(Throwable throwable) {

            }
          });
          futureMap.put(id, f);
          relevantFutures.add(f);
          if(colFam != null){
            cf.channel().writeAndFlush(new DefaultChicagoMessage(id, Op.TS_WRITE, colFam, key, value));
          }else {
            cf.channel().writeAndFlush(new DefaultChicagoMessage(id, Op.TS_WRITE, key, null, value));
          }
        }
      }
    }
    return Futures.successfulAsList(relevantFutures);
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

  public ListenableFuture<Boolean> deleteColFam(byte[] colFam) throws ChicagoClientTimeoutException, ChicagoClientException {
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
}
