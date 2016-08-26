package com.xjeffrose.chicago.client;

import com.xjeffrose.chicago.NodeListener;
import com.xjeffrose.chicago.TreeCacheInstance;
import com.xjeffrose.chicago.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class ClientNodeWatcher {
  private static final Logger log = LoggerFactory.getLogger(ClientNodeWatcher.class);
  private final static String NODE_LIST_PATH = "/chicago/node-list";
  public final static String REPLICATION_LOCK_PATH ="/chicago/replication-lock";
  private final CountDownLatch latch = new CountDownLatch(1);
  private final GenericListener genericListener = new GenericListener(NODE_LIST_PATH);
  private TreeCacheInstance nodeList;
  private TreeCacheInstance replicationPathTree;
  private ZkClient zkClient;
  private final List<NodeListener> listeners = Collections.synchronizedList(new ArrayList<>());
  private ConnectionPoolManager connectionPoolManager;

  public ClientNodeWatcher(ZkClient zkClient) {
    nodeList = new TreeCacheInstance(zkClient, NODE_LIST_PATH);
    this.zkClient = zkClient;
    this.replicationPathTree = new TreeCacheInstance(zkClient, REPLICATION_LOCK_PATH);
    nodeList.getCache().getListenable().addListener(new GenericListener(NODE_LIST_PATH));
  }

  public void registerListener(NodeListener listener){
    listeners.add(listener);
  }

  public void start() {
    try {
      nodeList.start();
      replicationPathTree.start();
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

  public void registerConnectionPoolManager(ConnectionPoolManager connectionPoolManager){
    this.connectionPoolManager = connectionPoolManager;
  }

  public void stop() {
    nodeList.getCache().getListenable().removeListener(genericListener);
    nodeList.stop();
    replicationPathTree.stop();
  }

  private void nodeAdded(String path) {
    String[] _path = path.split("/");
    if(connectionPoolManager != null){
      connectionPoolManager.checkConnection();
    }
  }

  public List<String> getReplicationPathData(String path){
      Map<String,ChildData> children = replicationPathTree.getCache().getCurrentChildren(path);
      if(children != null){
        return new ArrayList<>(children.keySet());
      }else{
        return  new ArrayList<>();
      }
  }

  private void nodeRemoved(String path) {
    String[] _path = path.split("/");
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
            String[] path = event.getData().getPath().split("/");
            String node = path[path.length-1];
            for(NodeListener listener : listeners){
              listener.nodeAdded(node);
            }
          }
          break;
        case NODE_REMOVED:
          if (initialized) {
            nodeRemoved(event.getData().getPath());
          }
          if (!NODE_LIST_PATH.equals(event.getData().getPath())) {
            String[] path = event.getData().getPath().split("/");
            String node = path[path.length-1];
            for(NodeListener listener : listeners){
              listener.nodeRemoved(node);
            }
          }
          break;
        case CONNECTION_RECONNECTED:
          connectionPoolManager.checkConnection();
        default: {
          log.info("Zk " + event.getType().name());
        }
      }
    }
  }


}
