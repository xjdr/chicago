package com.xjeffrose.chicago;

import com.google.common.hash.Funnels;
import com.google.common.primitives.Longs;
import com.xjeffrose.chicago.client.*;

import com.xjeffrose.chicago.db.RocksDBImpl;
import com.xjeffrose.chicago.db.StorageProvider;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeWatcher {
  private static final Logger log = LoggerFactory.getLogger(NodeWatcher.class);
  private final String NODE_LIST_PATH;
  private final String REPLICATION_LOCK_PATH;
  private final int quorum;
  private final CountDownLatch latch = new CountDownLatch(1);
  private final GenericListener genericListener = new GenericListener();
  private ChicagoClient chicagoClient;
  private TreeCacheInstance nodeList;
  private ZkClient zkClient;
  private StorageProvider db;
  private ExecutorService replicationWorker = Executors.newFixedThreadPool(5);
  private String advertisedEndpoint;

  public NodeWatcher(String nodeListPath, String replicationLockPath, int quorum) {
    NODE_LIST_PATH = nodeListPath;
    REPLICATION_LOCK_PATH = replicationLockPath;
    this.quorum = quorum;
  }

  /**
   * TODO: Refactor this into a constructor and a start method
   */
  public void refresh(ZkClient zkClient, StorageProvider db, String advertisedEndpoint) {
    nodeList = new TreeCacheInstance(zkClient, NODE_LIST_PATH);
    this.zkClient = zkClient;
    this.db = db;
    this.advertisedEndpoint = advertisedEndpoint;
    nodeList.getCache().getListenable().addListener(genericListener);
    try {
      this.chicagoClient = new ChicagoClient(zkClient.getConnectionString(), quorum);
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
    if (chicagoClient != null) {
      chicagoClient.stop();
    } else {
      System.out.println("No chicago client to stop");
    }
    zkClient = null;
    if (nodeList != null && nodeList.getCache() != null && nodeList.getCache().getListenable() != null) {
      nodeList.getCache().getListenable().removeListener(genericListener);
    } else {
      log.info("Couldn't remove genericListener without NPE");
    }
    replicationWorker.shutdownNow();
    if (nodeList != null) {
      nodeList.stop();
    }
  }

  private void redistributeKeys(String node, TreeCacheEvent.Type type) {
      log.info("Starting replication...");
      long startTime = System.currentTimeMillis();
      RendezvousHash rendezvousHash = new RendezvousHash(Funnels.stringFunnel(Charset.defaultCharset()), zkClient.list(NODE_LIST_PATH), quorum);
      RendezvousHash rendezvousHashnOld = new RendezvousHash(Funnels.stringFunnel(Charset.defaultCharset()), zkClient.list(NODE_LIST_PATH), quorum);
      switch(type){
        case NODE_ADDED: rendezvousHashnOld.remove(node);
                         break;
        case NODE_REMOVED: rendezvousHashnOld.add(node);
                           break;
      }

      //todo: Need to do faster replication with multi threading and future listeners.
      //For all the column family present on this server.
      db.getColFams().parallelStream().forEach(cf -> {
        List<String> newS = rendezvousHash.get(cf.getBytes());
        List<String> oldS = rendezvousHashnOld.get(cf.getBytes());
        String bounceLockPath = "";
        boolean bounceLock=false;
        newS.removeAll(oldS);

        if(type == TreeCacheEvent.Type.NODE_REMOVED){
          bounceLockPath = REPLICATION_LOCK_PATH + "/" + cf + "/" + node;
          zkClient.createLockPath(bounceLockPath, advertisedEndpoint, "BOUNCE_LOCK");
          bounceLock=true;
        }

        //For all the nodes that have been newly added to the hash.
        newS.forEach(s -> {
            if(!s.equals(advertisedEndpoint)) {
              log.info("Replicatng colFam " + cf + " to " + s);
              String lockPath = REPLICATION_LOCK_PATH + "/" + cf + "/" + s;
              try {
                zkClient.createLockPath(lockPath, advertisedEndpoint, "REPLICATION_LOCK");
                ChicagoClient c = new ChicagoClient((String) s);
                c.startAndWaitForNodes(1);
                byte[] offset = new byte[]{};
                List<byte[]> keys = db.getKeys(cf.getBytes(), offset);
                // Start replicating all the keys to the new server.
                while(!Arrays.equals(keys.get(keys.size()-1),offset)) {
                  for(byte[] k : keys){
                    log.debug("Writing key :"+ Longs.fromByteArray(k));
                    try {
                      c.tsWrite(cf.getBytes(), k, db.read(cf.getBytes(), k)).get();
                    } catch (Exception e) {
                      e.printStackTrace();
                      throw new ChicagoClientException(e.getCause().getMessage());
                    }
                    offset = k;
                  }
                  keys= db.getKeys(cf.getBytes(),offset);
                }
              } catch (ChicagoClientException e) {
                log.error("Something bad happened while replication");
              } catch (InterruptedException e) {
                e.printStackTrace();
              } finally {
                zkClient.deleteLockPath(lockPath, advertisedEndpoint);
              }
            }
          });

          //Remove the bounce lock.
          if(bounceLock){
            zkClient.deleteLockPath(bounceLockPath, advertisedEndpoint);
          }
        });
      log.info("Replication ended in " + (System.currentTimeMillis() - startTime) + "ms");
  }

  private void nodeAdded(String path, TreeCacheEvent.Type type) {
    String[] _path = path.split("/");
    replicationWorker.submit(() -> {
      redistributeKeys(_path[_path.length - 1],type);
    });
  }

  private void nodeRemoved(String path, TreeCacheEvent.Type type) {
    String[] _path = path.split("/");
    replicationWorker.submit(() -> {
      redistributeKeys(_path[_path.length - 1],type);
    });
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
