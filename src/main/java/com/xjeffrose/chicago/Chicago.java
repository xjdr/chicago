package com.xjeffrose.chicago;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;

public class Chicago {
  private static final Logger log = Logger.getLogger(Chicago.class.getName());
  private final static String ELECTION_PATH = "/chicago/chicago-elect";
  private final static String NODE_LIST_PATH = "/chicago/node-list";

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
    DBRouter dbRouter = new DBRouter(config);

    try {
      CuratorFramework curator = CuratorFrameworkFactory.newClient(config.getZkHosts(),
          2000, 10000, new ExponentialBackoffRetry(1000, 3));
      curator.start();
      curator.blockUntilConnected();

      LeaderSelector leaderSelector = new LeaderSelector(curator, ELECTION_PATH, new LeaderSelectorListener() {
        @Override
        public void takeLeadership(CuratorFramework curatorFramework) throws Exception {

        }

        @Override
        public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {

        }
      });

      leaderSelector.autoRequeue();
      leaderSelector.start();

      ZkClient zkClient = new ZkClient(curator);
      zkClient.getClient().create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(NODE_LIST_PATH + "/" + config.getDBBindIP(), null);

      dbRouter.run();
    } catch (Exception e) {
      System.exit(-1);
    }
  }


}
