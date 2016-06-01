package com.xjeffrose.chicago;

import com.google.common.hash.Funnels;
import com.xjeffrose.chicago.client.ChicagoClient;
import com.xjeffrose.chicago.client.RendezvousHash;
import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.rocksdb.ReadOptions;

public class NodeWatcher {
  private static final Logger log = LoggerFactory.getLogger(NodeWatcher.class);
  private final static String NODE_LIST_PATH = "/chicago/node-list";
  private final CountDownLatch latch = new CountDownLatch(1);
  private final GenericListener genericListener = new GenericListener();
  private ChicagoClient chicagoClient;
  private TreeCacheInstance nodeList;
  private ZkClient zkClient;
  private DBManager dbManager;
  private ChiConfig config;

  public NodeWatcher() {
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

  private void redistributeKeys() {
//      RendezvousHash rendezvousHash = new RendezvousHash(Funnels.stringFunnel(Charset.defaultCharset()), zkClient.list(NODE_LIST_PATH), config.getQuorum());
//    dbManager.getKeys(new ReadOptions()).forEach(xs -> {
//      rendezvousHash.get(xs).stream()
//          .filter(xxs -> xxs == config.getDBBindIP())
//          .forEach(xxs -> chicagoClient.write(xs, dbManager.read(finalMsg.getColFam(), xs)));
//    });
  }

  private void nodeAdded() {
    redistributeKeys();
  }

  private void nodeRemoved() {
    redistributeKeys();
  }

  private class GenericListener implements TreeCacheListener {
    private boolean initialized = false;

    public GenericListener() {
    }

    @Override
    public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent event) throws Exception {
      switch (event.getType()) {
        case INITIALIZED:
          if (initialized) {
            redistributeKeys();
          }
          latch.countDown();
          initialized = true;
          break;
        case NODE_ADDED:
          if (initialized) {
            nodeAdded();
          }
          break;
        case NODE_REMOVED:
          if (initialized) {
            nodeRemoved();
          }
          break;
        default: {
          log.info("Zk " + event.getType().name());
        }
      }
    }
  }


}
