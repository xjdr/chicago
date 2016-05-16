package com.xjeffrose.chicago;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;

public class ChiLeaderSelectorListener implements LeaderSelectorListener {
  boolean leader = false;

  @Override
  public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
    synchronized (this) {
      leader = true;
      try {
        while (true) {
          this.wait();
        }
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        leader = false;
      }
    }
  }

  @Override
  public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {

  }
}
