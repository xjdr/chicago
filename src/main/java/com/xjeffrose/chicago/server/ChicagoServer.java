package com.xjeffrose.chicago;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.io.IOException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import com.xjeffrose.chicago.*;

public class Chicago {
  private static final Logger log = Logger.getLogger(Chicago.class.getName());
  private final static String ELECTION_PATH = "/chicago/chicago-elect";
  private final static String NODE_LIST_PATH = "/chicago/node-list";

  private final ChiConfig config;
  private final CuratorFramework curator;
  private final LeaderSelector leaderSelector;
  private final ChiLeaderSelectorListener leaderListener = new ChiLeaderSelectorListener();
  private final ZkClient zkClient;
  private DBManager dbManager;
  private NodeWatcher nodeWatcher;
  private DBRouter dbRouter;


  public ChicagoServer(ChiConfig config) {
    this.config = config;
    curator = CuratorFrameworkFactory.newClient(config.getZkHosts(), 2000, 10000, new ExponentialBackoffRetry(1000, 3));
    leaderSelector = new LeaderSelector(curator, ELECTION_PATH, leaderListener);
    zkClient = new ZkClient(curator);
  }


  private void configureLeaderSelector() {
    leaderSelector.autoRequeue();
    leaderSelector.start();
    config.setLeaderSelector(leaderSelector);
  }

  private void configureZookeeper() {
    try {
      zkClient
        .getClient()
        .create()
        .creatingParentsIfNeeded()
        .withMode(CreateMode.EPHEMERAL)
        .forPath(NODE_LIST_PATH + "/" + config.getDBBindIP(), ConfigSerializer.serialize(config).getBytes());

      config.setZkClient(zkClient);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  public void start() {
    log.info("Starting Chicago, have a nice day");

    try {
      curator.start();
      curator.blockUntilConnected();

      configureLeaderSelector();

      configureZookeeper();

      dbManager = new DBManager(config);
      nodeWatcher = new NodeWatcher();
      nodeWatcher.refresh(zkClient, leaderSelector, dbManager, config);

      dbRouter = new DBRouter(config, dbManager);
      dbRouter.run();

      log.info("I am the Leader: " + leaderSelector.hasLeadership());
    } catch (Exception e) {
      log.error("Startup Error", e);
    }
  }

  public void stop() {
    log.info("Stopping Chicago!");
    try {
      zkClient.stop();
      dbRouter.close();
      dbManager.destroy();

    } catch (Exception e) {
      log.error("Shutdown Error", e);
    }

  }

}
