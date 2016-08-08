package com.xjeffrose.chicago.client;

import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.xjeffrose.chicago.ChiUtil;
import com.xjeffrose.chicago.ChicagoMessage;
import com.xjeffrose.chicago.DefaultChicagoMessage;
import com.xjeffrose.chicago.Op;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChicagoClient extends BaseChicagoClient implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(ChicagoClient.class);
//  private final ByteBuf buffer = Unpooled.buffer();
//  private int count = 0;

  public ChicagoClient(String zkConnectionString, int quorum) throws InterruptedException {
    super(zkConnectionString, quorum);
  }

  /*
   * Happy Path:
   * (deprecated) Delete -> send message to all (3) available nodes wait for all (3) responses to be true.
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
   * two phase commit with multiple nodes
   *  write (key, value)
   *  ack x 3 nodes
   *  ok x 3 nodes -> write request
   */

  public ChicagoClient(String address) throws InterruptedException {
    super(address);
  }

  ChicagoClient(List<EmbeddedChannel> hostPool, Map<UUID, SettableFuture<byte[]>> futureMap, int quorum) throws InterruptedException {
    super(hostPool, futureMap, quorum);
  }

  public ByteBuf aggregatedStream(byte[] key, byte[] offset){
    ByteBuf responseStream = Unpooled.directBuffer();
    aggregatedStream(key,offset,responseStream);
    return  responseStream;
  }

  public void aggregatedStream(byte[] key, byte[] offset, ByteBuf responseStream) {
    //System.out.println("New Stream called with key = "+ new String(key) +" and Offset ="+ Longs.fromByteArray(offset));
    zkClient.getChildren(REPLICATION_LOCK_PATH).stream()
      .filter(xs -> xs.startsWith(new String(key)))
      .collect(Collectors.toList())
      .stream()
      .parallel()
      .forEach(xs -> {
          try {
            FutureCallback<List<byte[]>> cb = new FutureCallback<List<byte[]>>() {
              @Override
              public void onSuccess(@Nullable List<byte[]> bytes) {
                byte[] resultArray = bytes.get(0);
                Long newOffset = Longs.fromByteArray(offset);
                if (resultArray != null) {
                  newOffset = ChiUtil.findOffset(resultArray);
                  if (newOffset != -1) {
                    responseStream.writeBytes(
                      new String(resultArray).split(ChiUtil.delimiter)[0].getBytes());
                  }

                  if (new String(resultArray).split(ChiUtil.delimiter)[0].contains("\0")) {
                    newOffset++;
                  }

                  Long lastOffset = lastOffsetMap.get(xs);
                  if (lastOffset != null && lastOffset.equals(newOffset)) {
                    //try {
                    //  //Thread.sleep(500);
                    //} catch (InterruptedException e) {
                    //  e.printStackTrace();
                    //}
                  } else {
                    lastOffsetMap.put(xs, newOffset);
                  }
                }

                try {
                  Futures.addCallback(stream(xs.getBytes(),Longs.toByteArray(newOffset)),this);
                } catch (ChicagoClientTimeoutException e) {
                  e.printStackTrace();
                }
              }

              @Override
              public void onFailure(Throwable throwable) {

              }
            };

            if (offset != null) {
              Futures.addCallback(stream(xs.getBytes(), offset), cb);
            } else {
              Futures.addCallback(stream(xs.getBytes()), cb);
            }
          } catch (ChicagoClientTimeoutException e) {
            e.printStackTrace();
          }
    });
  }

  public ListenableFuture<List<byte[]>> stream(byte[] key) throws ChicagoClientTimeoutException {
    return stream(key, null);
  }

  public ListenableFuture<List<byte[]>> stream(byte[] key, byte[] offset) throws ChicagoClientTimeoutException {
    List<ListenableFuture<byte[]>> relevantFutures = new ArrayList<>();
    List<String> hashList = getEffectiveNodes(key);
    String node = hashList.get(0);
    if (node == null) {
    } else {
      Channel ch = null;
      try {
        ch = connectionPoolMgr.getNode(node).get(TIMEOUT, TimeUnit.MILLISECONDS);;
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ExecutionException e) {
        e.printStackTrace();
      } catch (TimeoutException e) {
        e.printStackTrace();
      }
      if (ch.isWritable()) {
        UUID id = UUID.randomUUID();
        SettableFuture<byte[]> f = SettableFuture.create();
        //Futures.withTimeout(f, TIMEOUT, TimeUnit.MILLISECONDS, evg);
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
        },evg);
        futureMap.put(id, f);
        relevantFutures.add(f);
        ch.writeAndFlush(new DefaultChicagoMessage(id, Op.STREAM, key, null, offset)).addListener(new ChannelFutureListener() {
          final ChannelFutureListener writeComplete = new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
              if (!future.isSuccess()) {
                log.error("Server error writing :" + " For UUID" + id + " and key " + new String(key));
              }
            }
          };
          @Override
          public void operationComplete(ChannelFuture channelFuture) throws Exception {
            if(!channelFuture.isSuccess()) {
              if (channelFuture.channel().isWritable()) {
                channelFuture.channel().writeAndFlush(new DefaultChicagoMessage(id, Op.STREAM, key, null, offset)).addListener(writeComplete);
              } else {
                Channel _ch = connectionPoolMgr.getNode(node).get(TIMEOUT, TimeUnit.MILLISECONDS);
                if (_ch.isWritable()) {
                  _ch.writeAndFlush(new DefaultChicagoMessage(id, Op.STREAM, key, null, offset)).addListener(writeComplete);
                  connectionPoolMgr.releaseChannel(node, _ch);
                }
              }
            }
          }
        });
        connectionPoolMgr.releaseChannel(node, ch);

        evg.schedule(() -> {
          String node1 = hashList.get(1);
          if (node1 == null) {
          } else {
            Channel ch1 = null;
            try {
              ch1 = connectionPoolMgr.getNode(node1).get(TIMEOUT, TimeUnit.MILLISECONDS);;
            } catch (ChicagoClientTimeoutException e) {
              e.printStackTrace();
            } catch (InterruptedException e) {
              e.printStackTrace();
            } catch (ExecutionException e) {
              e.printStackTrace();
            } catch (TimeoutException e) {
              e.printStackTrace();
            }
            if (ch1.isWritable()) {
              UUID id1 = UUID.randomUUID();
              SettableFuture<byte[]> f1 = SettableFuture.create();
//              Futures.withTimeout(f1, TIMEOUT, TimeUnit.MILLISECONDS, evg);
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
              ch1.writeAndFlush(new DefaultChicagoMessage(id1, Op.STREAM, key, null, offset)).addListener(new ChannelFutureListener() {
                final ChannelFutureListener writeComplete = new ChannelFutureListener() {
                  @Override
                  public void operationComplete(ChannelFuture future) {
                    if (!future.isSuccess()) {
                      log.error("Server error writing :" + " For UUID" + id + " and key " + new String(key));
                    }
                  }
                };
                @Override
                public void operationComplete(ChannelFuture channelFuture) throws Exception {
                  if(!channelFuture.isSuccess()) {
                    if (channelFuture.channel().isWritable()) {
                      channelFuture.channel().writeAndFlush(new DefaultChicagoMessage(id, Op.STREAM, key, null, offset)).addListener(writeComplete);
                    } else {
                      Channel _ch = connectionPoolMgr.getNode(node).get(TIMEOUT, TimeUnit.MILLISECONDS);;
                      if (_ch.isWritable()) {
                        _ch.writeAndFlush(new DefaultChicagoMessage(id, Op.STREAM, key, null, offset)).addListener(writeComplete);
                        connectionPoolMgr.releaseChannel(node, _ch);
                      }
                    }
                  }
                }
              });
              connectionPoolMgr.releaseChannel(node, ch1);
            }
          }
        }, 2, TimeUnit.MILLISECONDS);

        return Futures.successfulAsList(relevantFutures);
      }
    }
    return null;
  }

  public ListenableFuture<List<byte[]>> read(byte[] key) throws ChicagoClientTimeoutException, InterruptedException, TimeoutException, ExecutionException {
    return read(ChiUtil.defaultColFam.getBytes(), key);
  }

  public ListenableFuture<List<byte[]>> read(byte[] colFam, byte[] key) throws ChicagoClientTimeoutException, InterruptedException, TimeoutException, ExecutionException {
//    ConcurrentLinkedDeque<byte[]> responseList = new ConcurrentLinkedDeque<>();
    List<ListenableFuture<byte[]>> relevantFutures = new ArrayList<>();
    List<String> hashList = getEffectiveNodes(colFam);
    String node = hashList.get(0);
    if (node == null) {
    } else {
      Channel ch = connectionPoolMgr.getNode(node).get(TIMEOUT, TimeUnit.MILLISECONDS);;
      if (ch.isWritable()) {
        UUID id = UUID.randomUUID();
        SettableFuture<byte[]> f = SettableFuture.create();
//        Futures.withTimeout(f, TIMEOUT, TimeUnit.MILLISECONDS, evg);
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
        ch.writeAndFlush(new DefaultChicagoMessage(id, Op.READ, colFam, key, null), ch.voidPromise());
        connectionPoolMgr.releaseChannel(node, ch);

        evg.schedule(() -> {
          String node1 = hashList.get(1);
          if (node1 == null) {
          } else {
            Channel ch1 = null;
            try {
              ch1 = connectionPoolMgr.getNode(node1).get(TIMEOUT, TimeUnit.MILLISECONDS);
            } catch (ChicagoClientTimeoutException e) {
              e.printStackTrace();
            } catch (InterruptedException e) {
              e.printStackTrace();
            } catch (ExecutionException e) {
              e.printStackTrace();
            } catch (TimeoutException e) {
              e.printStackTrace();
            }
            if (ch1.isWritable()) {
              UUID id1 = UUID.randomUUID();
              SettableFuture<byte[]> f1 = SettableFuture.create();
//              Futures.withTimeout(f1, TIMEOUT, TimeUnit.MILLISECONDS, evg);
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
              ch1.writeAndFlush(new DefaultChicagoMessage(id1, Op.READ, colFam, key, null), ch1.voidPromise());
              connectionPoolMgr.releaseChannel(node, ch1);
            }
          }
        }, 2, TimeUnit.MILLISECONDS);

        return Futures.successfulAsList(relevantFutures);
      }
    }
    return null;
  }

  public ListenableFuture<List<byte[]>> write(byte[] key, byte[] value) throws ChicagoClientTimeoutException, ChicagoClientException, InterruptedException, TimeoutException, ExecutionException {
    return write(ChiUtil.defaultColFam.getBytes(), key, value);
  }

  public ListenableFuture<List<byte[]>> write(byte[] colFam, byte[] key, byte[] value) throws ChicagoClientTimeoutException, ChicagoClientException, InterruptedException, TimeoutException, ExecutionException {
    return _write(colFam, key, value, 0);
  }

  private ListenableFuture<List<byte[]>> _write(byte[] colFam, byte[] key, byte[] value, int _retries) throws ChicagoClientTimeoutException, ChicagoClientException, InterruptedException, TimeoutException, ExecutionException {
//    final int retries = _retries;
    List<ListenableFuture<byte[]>> relevantFutures = new ArrayList<>();
//    final long startTime = System.currentTimeMillis();
    List<String> hashList = getEffectiveNodes(colFam);
    for (String node : hashList) {
      if (node == null) {
      } else {
        Channel ch = connectionPoolMgr.getNode(node).get(TIMEOUT, TimeUnit.MILLISECONDS);
        if (ch.isWritable()) {
          UUID id = UUID.randomUUID();
          SettableFuture<byte[]> f = SettableFuture.create();
//          Futures.withTimeout(f, TIMEOUT, TimeUnit.MILLISECONDS, evg);
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
          ch.writeAndFlush(new DefaultChicagoMessage(id, Op.WRITE, colFam, key, value)).addListener(new ChannelFutureListener() {
            final ChannelFutureListener writeComplete = new ChannelFutureListener() {
              @Override
              public void operationComplete(ChannelFuture future) {
                if (!future.isSuccess()) {
                  log.error("Server error writing :" + " For UUID" + id + " and key " + new String(key));
                }
              }
            };
            @Override
            public void operationComplete(ChannelFuture channelFuture) throws Exception {
              if(!channelFuture.isSuccess()) {
                if (channelFuture.channel().isWritable()) {
                  channelFuture.channel().writeAndFlush(new DefaultChicagoMessage(id, Op.WRITE, colFam, key, value)).addListener(writeComplete);
                } else {
                  Channel _ch = connectionPoolMgr.getNode(node).get(TIMEOUT, TimeUnit.MILLISECONDS);;
                  if (_ch.isWritable()) {
                    _ch.writeAndFlush(new DefaultChicagoMessage(id, Op.WRITE, colFam, key, value)).addListener(writeComplete);
                    connectionPoolMgr.releaseChannel(node, _ch);
                  }
                }
              }
            }
          });
          connectionPoolMgr.releaseChannel(node, ch);
        }
      }
    }
    return Futures.successfulAsList(relevantFutures);
  }

  public ListenableFuture<List<byte[]>> tsWrite(byte[] key, byte[] value) throws ChicagoClientTimeoutException, ChicagoClientException, InterruptedException, TimeoutException, ExecutionException {
    return _tsWrite(null,key,value,0);
  }

  public ListenableFuture<List<byte[]>> tsWrite(byte[] colFam, byte[] key, byte[] value) throws ChicagoClientTimeoutException, ChicagoClientException, InterruptedException, TimeoutException, ExecutionException {
    return _tsWrite(colFam, key, value, 0);
  }

  private ListenableFuture<List<byte[]>> _tsWrite(byte[] colFam, byte[] key, byte[] value, int _retries) throws ChicagoClientTimeoutException, ChicagoClientException, InterruptedException, TimeoutException, ExecutionException {
//    final int retries = _retries;
    List<ListenableFuture<byte[]>> relevantFutures = new ArrayList<>();
//    final long startTime = System.currentTimeMillis();
    List<String> hashList;
    if (colFam == null) {
      hashList = getEffectiveNodes(key);
    } else {
      hashList = getEffectiveNodes(colFam);
    }
    for (String node : hashList) {
      if (node == null) {
      } else {
        Channel ch = connectionPoolMgr.getNode(node).get(TIMEOUT, TimeUnit.MILLISECONDS);;
        if (ch.isWritable()) {
          UUID id = UUID.randomUUID();
          SettableFuture<byte[]> f = SettableFuture.create();
//          Futures.withTimeout(f, TIMEOUT, TimeUnit.MILLISECONDS, evg);
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
          if (colFam != null) {
            ch.writeAndFlush(new DefaultChicagoMessage(id, Op.TS_WRITE, colFam, key, value)).addListener(new ChannelFutureListener() {
              final ChannelFutureListener writeComplete = new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) {
                  if (!future.isSuccess()) {
                    log.error("Server error writing :" + " For UUID" + id + " and key " + new String(key));
                  }
                }
              };
              @Override
              public void operationComplete(ChannelFuture channelFuture) throws Exception {
                if(!channelFuture.isSuccess()) {
                  if (channelFuture.channel().isWritable()) {
                    channelFuture.channel().writeAndFlush(new DefaultChicagoMessage(id, Op.TS_WRITE, colFam, key, value)).addListener(writeComplete);
                  } else {
                    Channel _ch = connectionPoolMgr.getNode(node).get(TIMEOUT, TimeUnit.MILLISECONDS);;
                    if (_ch.isWritable()) {
                      _ch.writeAndFlush(new DefaultChicagoMessage(id, Op.TS_WRITE, colFam, key, value)).addListener(writeComplete);
                      connectionPoolMgr.releaseChannel(node, _ch);
                    }
                  }
                }
              }
            });
          } else {
            ch.writeAndFlush(new DefaultChicagoMessage(id, Op.TS_WRITE, key, null, value)).addListener(new ChannelFutureListener() {
              final ChannelFutureListener writeComplete = new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) {
                  if (!future.isSuccess()) {
                    log.error("Server error writing :" + " For UUID" + id + " and key " + new String(key));
                  }
                }
              };
              @Override
              public void operationComplete(ChannelFuture channelFuture) throws Exception {
                if(!channelFuture.isSuccess()) {
                  if (channelFuture.channel().isWritable()) {
                    channelFuture.channel().writeAndFlush(new DefaultChicagoMessage(id, Op.TS_WRITE, key, null, value)).addListener(writeComplete);
                  } else {
                    Channel _ch = connectionPoolMgr.getNode(node).get(TIMEOUT, TimeUnit.MILLISECONDS);;
                    if (_ch.isWritable()) {
                      _ch.writeAndFlush(new DefaultChicagoMessage(id, Op.TS_WRITE, key, null, value)).addListener(writeComplete);
                      connectionPoolMgr.releaseChannel(node, _ch);
                    }
                  }
                }
              }
            });
          }
          connectionPoolMgr.releaseChannel(node, ch);
        }
      }
    }
    return Futures.successfulAsList(relevantFutures);
  }


  public ListenableFuture<List<byte[]>> tsbatchWrite(byte[] key, byte[] value) throws ChicagoClientTimeoutException, ChicagoClientException {
    return chicagoBuffer.append(key , value);
  }

  @Deprecated
  public ListenableFuture<List<byte[]>> delete(byte[] key) throws ChicagoClientTimeoutException, ChicagoClientException, InterruptedException, TimeoutException, ExecutionException {
    return delete(ChiUtil.defaultColFam.getBytes(), key);
  }

  @Deprecated
  public ListenableFuture<List<byte[]>> delete(byte[] colFam, byte[] key) throws ChicagoClientTimeoutException, ChicagoClientException, InterruptedException, TimeoutException, ExecutionException {
    return _delete(colFam, key, 0);
  }

  public ListenableFuture<List<byte[]>> deleteColFam(byte[] colFam) throws ChicagoClientTimeoutException, ChicagoClientException, InterruptedException, TimeoutException, ExecutionException {
//    final int retries = 0;
    List<ListenableFuture<byte[]>> relevantFutures = new ArrayList<>();
//    final long startTime = System.currentTimeMillis();
    List<String> hashList = getEffectiveNodes(colFam);
    for (String node : hashList) {
      if (node == null) {
      } else {
        Channel ch = connectionPoolMgr.getNode(node).get(TIMEOUT, TimeUnit.MILLISECONDS);;
        if (ch.isWritable()) {
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
          ch.writeAndFlush(new DefaultChicagoMessage(id, Op.DELETE, colFam, null, null), ch.voidPromise());
          connectionPoolMgr.releaseChannel(node, ch);
        }
      }
    }
    return Futures.successfulAsList(relevantFutures);
  }

  private ListenableFuture<List<byte[]>> _delete(byte[] colFam, byte[] key, int _retries) throws ChicagoClientTimeoutException, ChicagoClientException, InterruptedException, TimeoutException, ExecutionException {
    final int retries = _retries;
    List<ListenableFuture<byte[]>> relevantFutures = new ArrayList<>();
    final long startTime = System.currentTimeMillis();
    List<String> hashList = getEffectiveNodes(colFam);
    for (String node : hashList) {
      if (node == null) {
      } else {
        Channel ch = connectionPoolMgr.getNode(node).get(TIMEOUT, TimeUnit.MILLISECONDS);
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
          ch.writeAndFlush(new DefaultChicagoMessage(id, Op.DELETE, colFam, key, null), ch.voidPromise());
          connectionPoolMgr.releaseChannel(node, ch);
        }
      }
//    }
    return Futures.successfulAsList(relevantFutures);
  }

  @Override
  public void close() throws Exception {
    stop();
  }
}
