package com.xjeffrose.chicago.server;

import com.xjeffrose.chicago.ChicagoPaxosClient;
import com.xjeffrose.chicago.NodeWatcher;
import com.xjeffrose.chicago.db.EncryptedStorageProvider;
import com.xjeffrose.chicago.db.InMemDBImpl;
import com.xjeffrose.chicago.db.RocksDBImpl;
import com.xjeffrose.chicago.db.StorageProvider;
import com.xjeffrose.chicago.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class ChicagoServer {
  private static final Logger log = LoggerFactory.getLogger(ChicagoServer.class.getName());
  private final static String ELECTION_PATH = "/chicago/chicago-elect";
  public final static String NODE_LIST_PATH = "/chicago/node-list";
  public final static String NODE_LOCK_PATH = "/chicago/replication-lock";

  public final ChiConfig config;
  private final ChicagoPaxosClient paxosClient;
  private final StorageProvider db;
  private final NodeWatcher nodeWatcher;
  private final DBRouter dbRouter;

  private  ZkClient zkClient;


  public ChicagoServer(ChiConfig config) {
    this.config = config;
    this.zkClient = new ZkClient(config.getZkHosts(),true);
    this.db = getStorageProvider(config);
    this.nodeWatcher = new NodeWatcher(NODE_LIST_PATH, NODE_LOCK_PATH, config.getQuorum());
    this.paxosClient = new ChicagoPaxosClient(config.getZkHosts(), config.getReplicaSize());
    this.dbRouter = new DBRouter(db, paxosClient);
  }

  private StorageProvider getStorageProvider(ChiConfig config) {
    if (config.isEncryptAtRest()) {
      if (config.isDatabaseMode()) {
        return new EncryptedStorageProvider(new RocksDBImpl(config));
      } else {
        return new EncryptedStorageProvider(new InMemDBImpl());
      }
    } else {
      if (config.isDatabaseMode()) {
        return new RocksDBImpl(config);
      } else {
        return new InMemDBImpl();
      }
    }
  }

  public void start() throws Exception {
    dbRouter.run();
    if(!zkClient.getClient().getState().equals(CuratorFrameworkState.STARTED)) {
      zkClient = new ZkClient(config.getZkHosts(), true);
      zkClient.start();
      paxosClient.start();
    }
    register(NODE_LIST_PATH, config, dbRouter.getDBBoundInetAddress(),zkClient);
    zkClient.electLeader(ELECTION_PATH);
    zkClient.createIfNotExist(NODE_LOCK_PATH,"");
    nodeWatcher.refresh(zkClient, db, getDBAddress());
    db.setZkClient(zkClient);
    ConnectionStateListener connectionStateListener = new ConnectionStateListener() {
      @Override
      public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
        switch (connectionState) {
          case RECONNECTED:
            register(ChicagoServer.NODE_LIST_PATH, config, dbRouter.getDBBoundInetAddress(), zkClient);
            break;
          default:
            break;
        }
      }
    };
    zkClient.getClient().getConnectionStateListenable().addListener(connectionStateListener);
  }

  public void register(String NODE_LIST_PATH, ChiConfig config, InetSocketAddress address, ZkClient client) {
    try {
      String path =
        NODE_LIST_PATH + "/" + address.getAddress().getHostAddress() + ":" + address.getPort();
      if (client.getClient().checkExists().forPath(path) != null) {
        client.getClient()
          .delete()
          .forPath(path);
      }
      client.getClient()
        .create()
        .creatingParentsIfNeeded()
        .withMode(CreateMode.EPHEMERAL)
        .forPath(path, config.toString().getBytes());
    } catch (Exception e) {
      log.error("Error registering Server", e);
      throw new RuntimeException(e);
    }
  }

  public void stop() {
    log.info("Stopping Chicago!");
    try {
      nodeWatcher.stop();
      if (zkClient != null) {
        zkClient.stop();
      }
      zkClient = null;
      dbRouter.close();
      db.close();

    } catch (Exception e) {
      log.error("Shutdown Error", e);
    }
  }

  public String getDBAddress(){
    return dbRouter.getDBBoundInetAddress().getAddress().getHostAddress()+ ":" + dbRouter.getDBBoundInetAddress().getPort();
  }
}
