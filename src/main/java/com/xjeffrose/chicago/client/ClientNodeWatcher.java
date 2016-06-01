package com.xjeffrose.chicago.client;

import com.xjeffrose.chicago.TreeCacheInstance;
import com.xjeffrose.chicago.ZkClient;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientNodeWatcher {
  public interface Listener {
    void nodeAdded();
    void nodeRemoved();
  }
  private static final Logger log = LoggerFactory.getLogger(ClientNodeWatcher.class);
  private final static String NODE_LIST_PATH = "/chicago/node-list";
  private final CountDownLatch latch = new CountDownLatch(1);
  private final GenericListener genericListener = new GenericListener(NODE_LIST_PATH);
  private TreeCacheInstance nodeList;
  private ZkClient zkClient;
  private RendezvousHash rendezvousHash;
  private final Listener listener;

  public ClientNodeWatcher(ZkClient zkClient, RendezvousHash rendezvousHash, Listener listener) {
    nodeList = new TreeCacheInstance(zkClient, NODE_LIST_PATH);
    this.zkClient = zkClient;
    this.rendezvousHash = rendezvousHash;
    this.listener = listener;
    nodeList.getCache().getListenable().addListener(new GenericListener(NODE_LIST_PATH));
  }

  public void start() {
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

  public void stop() {
    nodeList.getCache().getListenable().removeListener(genericListener);
    nodeList.stop();
  }

  private void nodeAdded(String path) {
    String[] _path = path.split("/");
    rendezvousHash.add(_path[_path.length - 1]);
  }

  private void nodeRemoved(String path) {
    String[] _path = path.split("/");
    rendezvousHash.remove(_path[_path.length - 1]);
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
          if (!NODE_LIST_PATH.equals(event.getData().getPath())) {
            if (listener != null) {
              listener.nodeAdded();
            }
          }
          break;
        case NODE_REMOVED:
          if (initialized) {
            nodeRemoved(event.getData().getPath());
          }
          if (!NODE_LIST_PATH.equals(event.getData().getPath())) {
            if (listener != null) {
              listener.nodeRemoved();
            }
          }
          break;
        default: {
          log.info("Zk " + event.getType().name());
        }
      }
    }
  }


}
