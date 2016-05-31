package com.xjeffrose.chicago;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Chicago {
  private static final Logger log = LoggerFactory.getLogger(Chicago.class.getName());
  private final static String ELECTION_PATH = "/chicago/chicago-elect";
  private final static String NODE_LIST_PATH = "/chicago/node-list";

//  private static ZkClient zkClient;
//  private static DBManager dbManager;
//  private static NodeWatcher nodeWatcher;
//  private static DBRouter dbRouter;
//  private static ChiConfig config;


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

    ChiConfig config = new ChiConfig(_conf);

    try {

      ZkClient zkClient = new ZkClient(config.getZkHosts());
      zkClient.start();
      zkClient.register(NODE_LIST_PATH, config);
      zkClient.electLeader(ELECTION_PATH);

      config.setZkClient(zkClient);

      DBManager dbManager = new DBManager(config);
      NodeWatcher nodeWatcher = new NodeWatcher();
      nodeWatcher.refresh(zkClient, dbManager, config);

      DBRouter dbRouter = new DBRouter(config, dbManager);
      dbRouter.run();

    } catch (Exception e) {
      log.error("Error Starting Chicago", e);
      System.exit(-1);
    }
  }

  public void stop() {
//    try {
//      zkClient.getClient().delete().forPath((NODE_LIST_PATH + "/" + config.getDBBindIP()));
//      zkClient.stop();
//      dbRouter.close();
//      dbManager.destroy();
//
//    } catch (Exception e) {
//      System.exit(-1);
//    }
  }

}
