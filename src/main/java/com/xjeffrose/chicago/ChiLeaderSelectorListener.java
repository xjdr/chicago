package com.xjeffrose.chicago;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;

public class ChiLeaderSelectorListener implements LeaderSelectorListener {
  String leader = null;

  @Override
  public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
    if (leader == null) {
      System.out.println("========= I am the leader now! ===========");
      leader = "me";
    }
  }

  @Override
  public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {

  }
}
