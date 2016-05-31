package com.xjeffrose.chicago;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChiLeaderSelectorListener implements LeaderSelectorListener {
  private static final Logger log = LoggerFactory.getLogger(ChiLeaderSelectorListener.class.getName());
  private Lock lock = new ReentrantLock();
  private Condition dropLeadership = lock.newCondition();

  public void relinquish() throws Exception {
    lock.lock();
    try {
      dropLeadership.signal();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
    lock.lock();
    try {
      dropLeadership.await();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
  }
}
