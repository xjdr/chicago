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
//    refreshPool();
  }


  private List<String> buildNodeList() {
    return zkClient.list(NODE_LIST_PATH);
  }

  public byte[] read(byte[] key) {
    List<byte[]> responseList = new ArrayList<>();

    rendezvousHash.get(key).stream().filter(x -> x!=null).forEach(xs -> {
      ChannelFuture cf = connectionPoolMgr.getNode((String) xs);
      if (cf.channel().isWritable()) {
        cf.channel().writeAndFlush(new DefaultChicagoMessage(Op.READ, key, null));
        responseList.add((byte[]) connectionPoolMgr.getListener((String) xs).getResponse());
      }

    });

    return responseList.stream().filter(x-> x!= null).findFirst().orElse(null);
  }

  public boolean write(byte[] key, byte[] value) {
    List<Boolean> responseList = new ArrayList<>();
    long start_time = System.currentTimeMillis();
      rendezvousHash.get(key).stream().filter(x -> x!=null).forEach(xs -> {
        ChannelFuture cf = connectionPoolMgr.getNode((String) xs);
        if (cf.channel().isWritable()) {
          cf.channel().writeAndFlush(new DefaultChicagoMessage(Op.WRITE, key, value));
          responseList.add(connectionPoolMgr.getListener((String) xs).getStatus());
        }
    });
    System.out.println("return response in "+ (System.currentTimeMillis() - start_time));
    return responseList.stream().allMatch(b -> b);
  }

  public boolean delete(byte[] key) {
    List<Boolean> responseList = new ArrayList<>();

    rendezvousHash.get(key).stream().filter(x -> x != null).forEach(xs -> {
      ChannelFuture cf = connectionPoolMgr.getNode((String) xs);
      if (cf.channel().isWritable()) {
        cf.channel().writeAndFlush(new DefaultChicagoMessage(Op.DELETE, key, null));
        responseList.add(connectionPoolMgr.getListener((String) xs).getStatus());
      }

    });

    return responseList.stream().allMatch(b -> b);
  }
}
