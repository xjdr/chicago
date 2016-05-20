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
import org.apache.log4j.Logger;


public class ChicagoClient {
  private static final Logger log = Logger.getLogger(ChicagoClient.class);
  private final static String NODE_LIST_PATH = "/chicago/node-list";

  private final InetSocketAddress single_server;
  private final RendezvousHash rendezvousHash;
  private final ClientNodeWatcher clientNodeWatcher;
  private final ZkClient zkClient;
  private final ConnectionPoolManager connectionPoolMgr;


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
    List<byte[]> responseList = new ArrayList<>();

    if (single_server != null) {
    }

    try {

      List<String> hashList = rendezvousHash.get(key);

      for (String node : hashList) {
        if (node == null) {

        } else {
          ChannelFuture cf = connectionPoolMgr.getNode(node);
          if (cf.channel().isWritable()) {
            cf.channel().writeAndFlush(new DefaultChicagoMessage(Op.READ, colFam, key, null));
            responseList.add((byte[]) connectionPoolMgr.getListener(node).getResponse());
          }
        }
      }
    } catch (ChicagoClientTimeoutException e) {
      log.error("Client Timeout", e);
      return null;
    }


    return responseList.stream().findFirst().orElse(null);
  }

  public boolean write(byte[] key, byte[] value) {
    return write("chicago".getBytes(), key, value);
  }

  public boolean write(byte[] colFam, byte[] key, byte[] value) {
    List<Boolean> responseList = new ArrayList<>();

    if (single_server != null) {
//      connect(single_server, Op.WRITE, key, value, listener);
    }
    long start_time = System.currentTimeMillis();
    try {

      List<String> hashList = rendezvousHash.get(key);

      for (String node : hashList) {
        if (node == null) {

        } else {
          ChannelFuture cf = connectionPoolMgr.getNode(node);
          if (cf.channel().isWritable()) {
            cf.channel().writeAndFlush(new DefaultChicagoMessage(Op.WRITE, colFam, key, value));
            responseList.add(connectionPoolMgr.getListener(node).getStatus());
          }
        }
      }

    } catch (ChicagoClientTimeoutException e) {
      log.error("Client Timeout", e);
      return false;
    }
    long diff = System.currentTimeMillis() - start_time;
    System.out.println("ChicagoClient write time = " + diff);
    return responseList.stream().allMatch(b -> b);
  }

  public boolean delete(byte[] key) {
    return delete("chicago".getBytes(), key);
  }

  public boolean delete(byte[] colFam, byte[] key) {
    List<Boolean> responseList = new ArrayList<>();

    try {

      List<String> hashList = rendezvousHash.get(key);

      for (String node : hashList) {
        if (node == null) {

        } else {
          ChannelFuture cf = connectionPoolMgr.getNode(node);
          if (cf.channel().isWritable()) {
            cf.channel().writeAndFlush(new DefaultChicagoMessage(Op.DELETE, colFam, key, null));
            responseList.add(connectionPoolMgr.getListener(node).getStatus());
          }
        }
      }

    } catch (ChicagoClientTimeoutException e) {
      log.error("Client Timeout", e);
      return false;
    }

    return responseList.stream().allMatch(b -> b);
  }
}
