package com.xjeffrose.chicago;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.log4j.Logger;

public class ChiLeaderSelectorListener implements LeaderSelectorListener {
  private static final Logger log = Logger.getLogger(ChiLeaderSelectorListener.class.getName());

  boolean leader = false;

  @Override
  public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
    synchronized (this) {
      leader = true;
      log.info("I am the Leader: " + leader);
      try {
        while (true) {
          this.wait();
        }
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        leader = false;
        log.info("I am the Leader: " + leader);
      }
    }
  }

  @Override
  public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
  }
}
