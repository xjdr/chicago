package com.xjeffrose.chicago.server;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.io.IOException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.CreateMode;
import com.xjeffrose.chicago.*;

public class ChicagoServer {
  private static final Logger log = LoggerFactory.getLogger(ChicagoServer.class.getName());
  private final static String ELECTION_PATH = "/chicago/chicago-elect";
  private final static String NODE_LIST_PATH = "/chicago/node-list";
  private final static String NODE_LOCK_PATH = "/chicago/replication-lock";

  public final ChiConfig config;
  private ZkClient zkClient;
  private final DBManager dbManager;
  private final NodeWatcher nodeWatcher;
  private final DBRouter dbRouter;
  public final DBLog dbLog = new DBLog();

  public ChicagoServer(ChiConfig config) {
    this.config = config;
    zkClient = new ZkClient(config.getZkHosts());
    dbManager = new DBManager(config);
    nodeWatcher = new NodeWatcher(NODE_LIST_PATH,NODE_LOCK_PATH);
    dbRouter = new DBRouter(config, dbManager, dbLog);
  }
  public void start() throws Exception {
    dbRouter.run();
    if(!zkClient.getClient().getState().equals(CuratorFrameworkState.STARTED)) {
      zkClient = new ZkClient(config.getZkHosts());
      zkClient.start();
    }
    zkClient.register(NODE_LIST_PATH, config, dbRouter.getDBBoundInetAddress());
    zkClient.electLeader(ELECTION_PATH);
    nodeWatcher.refresh(zkClient, dbManager, config);
  }
  public void stop() {
    log.info("Stopping Chicago!");
    try {
      nodeWatcher.stop();
      zkClient.stop();
      dbRouter.close();
      dbManager.destroy();

    } catch (Exception e) {
      log.error("Shutdown Error", e);
    }
  }

  public String getDBAddress(){
    return dbRouter.getDBBoundInetAddress().getAddress().getHostAddress()+ ":" + dbRouter.getDBBoundInetAddress().getPort();
  }
}
