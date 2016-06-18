package com.xjeffrose.chicago;

import com.google.common.hash.Funnels;
import com.google.common.primitives.Ints;
import com.xjeffrose.chicago.client.*;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.rocksdb.ReadOptions;

public class NodeWatcher {
  private static final Logger log = LoggerFactory.getLogger(NodeWatcher.class);
  private final String NODE_LIST_PATH;
  private final String REPLICATION_LOCK_PATH;
  private final CountDownLatch latch = new CountDownLatch(1);
  private final GenericListener genericListener = new GenericListener();
  private ChicagoClient chicagoClient;
  private TreeCacheInstance nodeList;
  private ZkClient zkClient;
  private DBManager dbManager;
  private ChiConfig config;

  public NodeWatcher(String nodeListPath, String replicationLockPath) {
    NODE_LIST_PATH = nodeListPath;
    REPLICATION_LOCK_PATH = replicationLockPath;
  }

  /**
   * TODO: Refactor this into a constructor and a start method
   */
  public void refresh(ZkClient zkClient, DBManager dbManager, ChiConfig config) {
    nodeList = new TreeCacheInstance(zkClient, NODE_LIST_PATH);
    this.zkClient = zkClient;
    this.dbManager = dbManager;
    this.config = config;
    nodeList.getCache().getListenable().addListener(genericListener);
    try {
      this.chicagoClient = new ChicagoClient(zkClient.getConnectionString(), config.getQuorum());
      chicagoClient.start();
      nodeList.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    try {
      latch.await();
      log.info("NodeWatcher initialization completed");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public void stop() throws Exception {
    log.info("Nodewatcher stopping");
    chicagoClient.stop();
    nodeList.getCache().getListenable().removeListener(genericListener);
    nodeList.stop();
  }

  private void redistributeKeys(String node, TreeCacheEvent.Type type) {
      log.info("Starting replication...");
      RendezvousHash rendezvousHash = new RendezvousHash(Funnels.stringFunnel(Charset.defaultCharset()), zkClient.list(NODE_LIST_PATH), config.getQuorum());
      RendezvousHash rendezvousHashnOld = new RendezvousHash(Funnels.stringFunnel(Charset.defaultCharset()), zkClient.list(NODE_LIST_PATH), config.getQuorum());
      switch(type){
        case NODE_ADDED: rendezvousHashnOld.remove(node);
                         break;
        case NODE_REMOVED: rendezvousHashnOld.add(node);
                           break;
      }

      dbManager.getColFams().forEach(cf -> {
        List<String> newS = rendezvousHash.get(cf.getBytes());
        List<String> oldS = rendezvousHashnOld.get(cf.getBytes());
        newS.removeAll(oldS);
        newS.forEach(s -> {
            if(!s.equals(config.getDBBindEndpoint())) {
              try {
                log.info("Replicatng colFam " + cf + " to " + s);
                zkClient.createIfNotExist(REPLICATION_LOCK_PATH + "/" + cf + "/" + s,config.getDBBindEndpoint());
                ChicagoTSClient c = new ChicagoTSClient((String) s);
                byte[] offset = new byte[]{};
                List<byte[]> keys = dbManager.getKeys(cf.getBytes(), offset);
                while(!Arrays.equals(keys.get(keys.size()-1),offset)) {
                  for(byte[] k : keys){
                    log.info("Writing key :"+Ints.fromByteArray(k));
                    try {
                      c._write(cf.getBytes(), k, dbManager.read(cf.getBytes(), k)).get();
                    } catch (ChicagoClientTimeoutException e) {
                      e.printStackTrace();
                    } catch (ChicagoClientException e) {
                      e.printStackTrace();
                    } catch (ExecutionException e) {
                      e.printStackTrace();
                    }
                    offset = k;
                  }
                  keys=dbManager.getKeys(cf.getBytes(),offset);
                }
                zkClient.delete(REPLICATION_LOCK_PATH + "/" + cf + "/" + s);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            }
          });
        });

//      RendezvousHash rendezvousHash = new RendezvousHash(Funnels.stringFunnel(Charset.defaultCharset()), zkClient.list(NODE_LIST_PATH), config.getQuorum());
//      dbManager.getKeys(new ReadOptions()).forEach(xs -> {
//        rendezvousHash.get(xs).stream()
//            .filter(xxs -> xxs == config.getDBBindIP())
//            .forEach(xxs -> chicagoClient.write(xs, dbManager.read(finalMsg.getColFam(), xs)));
//      });
  }

  private void nodeAdded(String path, TreeCacheEvent.Type type) {
    String[] _path = path.split("/");
    redistributeKeys(_path[_path.length - 1],type);
  }

  private void nodeRemoved(String path, TreeCacheEvent.Type type) {
    String[] _path = path.split("/");
    redistributeKeys(_path[_path.length - 1],type);
  }

  private class GenericListener implements TreeCacheListener {
    private boolean initialized = false;

    public GenericListener() {
    }

    @Override
    public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent event) throws Exception {
      switch (event.getType()) {
        case INITIALIZED:
          latch.countDown();
          initialized = true;
          break;
        case NODE_ADDED:
          if (initialized) {
            nodeAdded(event.getData().getPath(),event.getType());
          }
          break;
        case NODE_REMOVED:
          if (initialized) {
            nodeRemoved(event.getData().getPath(), event.getType());
          }
          break;
        default: {
          log.info("Zk " + event.getType().name());
        }
      }
    }
  }


}
