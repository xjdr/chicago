package com.xjeffrose.chicago.server;

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
  private final RocksDBImpl rocksDbImpl;
  private final NodeWatcher nodeWatcher;
  private final DBRouter dbRouter;
  public final DBLog dbLog = new DBLog();

  public ChicagoServer(ChiConfig config) {
    this.config = config;
    zkClient = new ZkClient(config.getZkHosts(),true);
    rocksDbImpl = new RocksDBImpl(config);
    nodeWatcher = new NodeWatcher(NODE_LIST_PATH, NODE_LOCK_PATH, config.getQuorum());
    dbRouter = new DBRouter(config, rocksDbImpl, dbLog);
//    config.setZkClient(zkClient);
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
    nodeWatcher.refresh(zkClient, rocksDbImpl, getDBAddress());
    rocksDbImpl.setZkClient(zkClient);
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
      rocksDbImpl.destroy();

    } catch (Exception e) {
      log.error("Shutdown Error", e);
    }
  }

  public String getDBAddress(){
    return dbRouter.getDBBoundInetAddress().getAddress().getHostAddress()+ ":" + dbRouter.getDBBoundInetAddress().getPort();
  }
}
