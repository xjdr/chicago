package com.xjeffrose.chicago;

import com.google.common.hash.Funnels;
import com.xjeffrose.chicago.client.*;

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
      log.info("Starting replication...");
      RendezvousHash rendezvousHash = new RendezvousHash(Funnels.stringFunnel(Charset.defaultCharset()), zkClient.list(NODE_LIST_PATH), config.getQuorum());
      dbManager.getColFams().forEach(cf -> {
        rendezvousHash.get(cf.getBytes())
          .forEach(s -> {
            if(!s.equals(config.getDBBindIP()+":"+config.getDBPort())) {
              try {
                log.info("Replicatng colFam " + cf + " to " + s);
                ChicagoTSClient c = new ChicagoTSClient((String) s);
                dbManager.getKeys(cf.getBytes()).forEach(k -> {
                  try {
                    c._write(cf.getBytes(), k, dbManager.read(cf.getBytes(), k));
                  } catch (ChicagoClientTimeoutException e) {
                    e.printStackTrace();
                  } catch (ChicagoClientException e) {
                    e.printStackTrace();
                  }
                });
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

  private void nodeAdded(String path) {
    String[] _path = path.split("/");
    redistributeKeys();
  }

  private void nodeRemoved(String path) {
    String[] _path = path.split("/");
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
            nodeAdded(event.getData().getPath());
          }
          break;
        case NODE_REMOVED:
          if (initialized) {
            nodeRemoved(event.getData().getPath());
          }
          break;
        default: {
          log.info("Zk " + event.getType().name());
        }
      }
    }
  }


}
