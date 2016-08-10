package com.xjeffrose.chicago.server;

import com.xjeffrose.chicago.db.EncryptedStorageProvider;
import com.xjeffrose.chicago.db.InMemDBImpl;
import com.xjeffrose.chicago.db.RocksDBImpl;
import com.xjeffrose.chicago.db.StorageProvider;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.xjeffrose.chicago.*;

public class ChicagoServer {
  private static final Logger log = LoggerFactory.getLogger(ChicagoServer.class.getName());
  private final static String ELECTION_PATH = "/chicago/chicago-elect";
  public final static String NODE_LIST_PATH = "/chicago/node-list";
  public final static String NODE_LOCK_PATH = "/chicago/replication-lock";

  public final ChiConfig config;
  private ZkClient zkClient;
  private final StorageProvider db;
  private final NodeWatcher nodeWatcher;
  private final DBRouter dbRouter;

  public ChicagoServer(ChiConfig config) {
    this.config = config;
    zkClient = new ZkClient(config.getZkHosts(),true);
    db = getStorageProvider(config);
    nodeWatcher = new NodeWatcher(NODE_LIST_PATH, NODE_LOCK_PATH, config.getQuorum());
    dbRouter = new DBRouter(db);
//    config.setZkClient(zkClient);
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
//      config.setZkClient(zkClient);
      zkClient.start();
    }
    zkClient.register(NODE_LIST_PATH, config, dbRouter.getDBBoundInetAddress());
    zkClient.electLeader(ELECTION_PATH);
    zkClient.createIfNotExist(NODE_LOCK_PATH,"");
    nodeWatcher.refresh(zkClient, db, getDBAddress());
    db.setZkClient(zkClient);
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
