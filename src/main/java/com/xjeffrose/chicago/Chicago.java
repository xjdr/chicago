package com.xjeffrose.chicago;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;

public class Chicago {
  private static final Logger log = Logger.getLogger(Chicago.class.getName());
  private final static String ELECTION_PATH = "/chicago/chicago-elect";
  private final static String NODE_LIST_PATH = "/chicago/node-list";

  private static ZkClient zkClient;
  private static DBManager dbManager;
  private static NodeWatcher nodeWatcher;
  private static DBRouter dbRouter;
  private static ChiConfig config;


  public static void main(String[] args) {
    log.info("Starting Chicago, have a nice day");

    Config _conf;

    if (args.length > 0) {
      try {
        _conf = ConfigFactory.parseFile(new File(args[1]));
      } catch (Exception e) {
        _conf = ConfigFactory.parseFile(new File("application.conf"));
      }
    } else {
      _conf = ConfigFactory.parseFile(new File("test.conf"));
    }

    config = new ChiConfig(_conf);

    try {
      CuratorFramework curator = CuratorFrameworkFactory.newClient(config.getZkHosts(),
          2000, 10000, new ExponentialBackoffRetry(1000, 3));
      curator.start();
      curator.blockUntilConnected();

      LeaderSelector leaderSelector = new LeaderSelector(curator, ELECTION_PATH, new ChiLeaderSelectorListener());

      leaderSelector.autoRequeue();
      leaderSelector.start();

      zkClient = new ZkClient(curator);
      zkClient
          .getClient()
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.EPHEMERAL)
          .forPath(NODE_LIST_PATH + "/" + config.getDBBindIP(), ConfigSerializer.serialize(config).getBytes());

      config.setLeaderSelector(leaderSelector);
      config.setZkClient(zkClient);

      dbManager = new DBManager(config);
      nodeWatcher = new NodeWatcher();
      nodeWatcher.refresh(zkClient, leaderSelector, dbManager, config);

      dbRouter = new DBRouter(config, dbManager);
      dbRouter.run();

      log.info("I am the Leader: " + leaderSelector.hasLeadership());
    } catch (Exception e) {
      System.exit(-1);
    }
  }

  public void stop() {
    try {
      zkClient.getClient().delete().forPath((NODE_LIST_PATH + "/" + config.getDBBindIP()));
      zkClient.stop();
      dbRouter.close();
      dbManager.destroy();

    } catch (Exception e) {
      System.exit(-1);
    }
  }

}
