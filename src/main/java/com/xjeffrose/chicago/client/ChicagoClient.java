package com.xjeffrose.chicago.client;

import com.google.common.hash.Funnels;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.log4j.Logger;

public class ChicagoClient {
  private static final Logger log = Logger.getLogger(ChicagoClient.class);
  private final static String NODE_LIST_PATH = "/chicago/node-list";

  private final ExecutorService exe = Executors.newFixedThreadPool(20);

  private final InetSocketAddress single_server;
  private final RendezvousHash rendezvousHash;
  private final ClientNodeWatcher clientNodeWatcher;
  private final ZkClient zkClient;
  private final ConnectionPoolManager connectionPoolMgr;

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
    ArrayList<String> nodeList = new ArrayList<>();
    nodeList.add(server.getHostName());
    this.rendezvousHash = new RendezvousHash(Funnels.stringFunnel(Charset.defaultCharset()), nodeList);
    this.clientNodeWatcher = null;
    connectionPoolMgr = new ConnectionPoolManager(server.getHostName());
  }

  public ChicagoClient(String zkConnectionString) throws InterruptedException {

    this.single_server = null;
    this.zkClient = new ZkClient(zkConnectionString);
    zkClient.start();

    this.rendezvousHash = new RendezvousHash(Funnels.stringFunnel(Charset.defaultCharset()), buildNodeList());
    this.clientNodeWatcher = new ClientNodeWatcher();
    clientNodeWatcher.refresh(zkClient, rendezvousHash);
    this.connectionPoolMgr = new ConnectionPoolManager(zkClient);
  }


  private List<String> buildNodeList() {
    return zkClient.list(NODE_LIST_PATH);
  }

  public byte[] read(byte[] key) {
    return read("chicago".getBytes(), key);
  }

  public byte[] read(byte[] colFam, byte[] key) {
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
            exe.execute(new Runnable() {
              @Override
              public void run() {
                try {
                  UUID id = UUID.randomUUID();
                  Listener listener = connectionPoolMgr.getListener(node);
                  cf.channel().writeAndFlush(new DefaultChicagoMessage(id, Op.READ, colFam, key, null));
                  listener.addID(id);
                  responseList.add((byte[]) listener.getResponse(id));
                } catch (ChicagoClientTimeoutException e) {
                  log.error("You are a READING asshole");
                }
              }
            });
          }
        }
      }

    } catch (ChicagoClientTimeoutException e) {
      log.error("Client Timeout", e);
      return null;
    }

    while (responseList.isEmpty()) {
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    return responseList.stream().findFirst().orElse(null);
  }

  public boolean write(byte[] key, byte[] value) {
    return write("chicago".getBytes(), key, value);
  }

  public boolean write(byte[] colFam, byte[] key, byte[] value) {
    ConcurrentLinkedDeque<Boolean> responseList = new ConcurrentLinkedDeque<>();

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
            exe.execute(new Runnable() {
              @Override
              public void run() {
            try {
                  UUID id = UUID.randomUUID();
                  Listener listener = connectionPoolMgr.getListener(node);
                  cf.channel().writeAndFlush(new DefaultChicagoMessage(id, Op.WRITE, colFam, key, value));
                  listener.addID(id);
                  responseList.add(listener.getStatus(id));
                } catch (ChicagoClientTimeoutException e) {
                  log.error("You are a WRITING asshole");
                }
              }
            });
          }
        }
      }

    } catch (ChicagoClientTimeoutException e) {
      log.error("Client Timeout", e);
      return false;
    }

    while (responseList.size() < 3) {
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    return responseList.stream().allMatch(b -> b);
  }

  public boolean delete(byte[] key) {
    return delete("chicago".getBytes(), key);
  }

  public boolean delete(byte[] colFam, byte[] key) {
    ConcurrentLinkedDeque<Boolean> responseList = new ConcurrentLinkedDeque<>();

    try {

      List<String> hashList = rendezvousHash.get(key);

      for (String node : hashList) {
        if (node == null) {

        } else {
          ChannelFuture cf = connectionPoolMgr.getNode(node);
          if (cf.channel().isWritable()) {
            exe.execute(new Runnable() {
              @Override
              public void run() {
                try {
                  UUID id = UUID.randomUUID();
                  Listener listener = connectionPoolMgr.getListener(node);
                  cf.channel().writeAndFlush(new DefaultChicagoMessage(id, Op.DELETE, colFam, key, null));
                  listener.addID(id);
                  responseList.add(listener.getStatus(id));
                } catch (ChicagoClientTimeoutException e) {
                  log.error("You are a DELETING asshole");
                }
              }
            });
          }
        }
      }

    } catch (ChicagoClientTimeoutException e) {
      log.error("Client Timeout", e);
      return false;
    }

    while (responseList.size() < 3) {
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    return responseList.stream().allMatch(b -> b);
  }
}
