package com.xjeffrose.chicago.client;

import com.xjeffrose.chicago.TreeCacheInstance;
import com.xjeffrose.chicago.ZkClient;
import java.util.concurrent.CountDownLatch;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.log4j.Logger;

public class ClientNodeWatcher {
  private static final Logger log = Logger.getLogger(ClientNodeWatcher.class);
  private final static String NODE_LIST_PATH = "/chicago/node-list";
  private final CountDownLatch latch = new CountDownLatch(1);
  private TreeCacheInstance nodeList;
  private ZkClient zkClient;
  private RendezvousHash rendezvousHash;

  public ClientNodeWatcher() {
  }

  public void refresh(ZkClient zkClient, RendezvousHash rendezvousHash) {
    nodeList = new TreeCacheInstance(zkClient, NODE_LIST_PATH);
    this.zkClient = zkClient;
    this.rendezvousHash = rendezvousHash;

    nodeList.getCache().getListenable().addListener(new GenericListener(NODE_LIST_PATH));
    try {
      nodeList.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    try {
      latch.await();
      log.info("ClientNodeWatcher initialization completed");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void nodeAdded(String path) {
    rendezvousHash.add(path.substring(path.lastIndexOf('/')+1));
  }

  private void nodeRemoved(String path) {
    rendezvousHash.remove(path.substring(path.lastIndexOf('/')+1));
  }

  private class GenericListener implements TreeCacheListener {
    private boolean initialized = false;
    private String path;

    public GenericListener(String path) {
      this.path = path;
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
