package com.xjeffrose.chicago;

import java.nio.charset.Charset;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkClient  implements  AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(ZkClient.class.getName());
  private LeaderSelector leaderSelector;
  private final ChiLeaderSelectorListener leaderListener = new ChiLeaderSelectorListener();
  private CuratorFramework client;
  private String connectionString;
  private final boolean isServer;

  public ZkClient(CuratorFramework client, boolean isServer) {
    this.client = client;
    this.connectionString = client.getZookeeperClient().getCurrentConnectionString();
    this.isServer = isServer;
  }

  public ZkClient(String serverSet, boolean isServer) {
    connectionString = serverSet;
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    client = CuratorFrameworkFactory.newClient(serverSet, retryPolicy);
    this.isServer = isServer;
  }

  public void rebuild() {
    client.close();
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 4);
    client = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
  }

  public void electLeader(String ELECTION_PATH) {
    leaderSelector = new LeaderSelector(client, ELECTION_PATH, leaderListener);
    leaderSelector.autoRequeue();
    leaderSelector.start();
  }

  public boolean isLeader(){
    return leaderSelector.hasLeadership();
  }

  public void start() throws InterruptedException {
    client.start();
  }

  public void stop() throws Exception {
    leaderListener.relinquish();
    if (leaderSelector != null) {
      // TODO(CK): this is throwing an exception, not sure how to do this properly.
      leaderSelector.close();
    }
    client.close();
  }

  public void set(String path, String data) {
    try {
      client.create().creatingParentsIfNeeded().forPath(path, data.getBytes());
    } catch (Exception e) {
      //throw new RuntimeException(e);
    }
  }

  public String get(String path) {
    try {
      return new String(client.getData().forPath(path), Charset.forName("UTF-8"));
    } catch (Exception e) {

      //TODO: I need to deal with the error better
//      log.severe("No node for for: " + path);
//      throw new RuntimeException(e);
    }
    return null;
  }

  public void set(String path, String data, boolean compress) {
    try {
      client.create().compressed().creatingParentsIfNeeded().forPath(path, data.getBytes());
    } catch (Exception e) {
      //throw new RuntimeException(e);
    }
  }

  public String get(String path, boolean compress) {
    try {
      return new String(client.getData().decompressed().forPath(path), Charset.forName("UTF-8"));
    } catch (Exception e) {
      //throw new RuntimeException(e);
    }
    return null;
  }

  public List<String> list(String path) {
    try {
      if (client.checkExists().forPath(path) != null) {
        return client.getChildren().forPath(path);
      }
    } catch (Exception e) {
      //throw new RuntimeException(e);
    }
    return new ArrayList<String>();
  }

  public List<String> getChildren(String path) {
    try {
      return client.getChildren().forPath(path);
    } catch (Exception e) {
      //TODO: I need to deal with the error better
//      log.severe("No node for for: " + path);
//      throw new RuntimeException(e);
    }
    return null;
  }

  public boolean createIfNotExist(String path, String data){
    try {
      if (client.checkExists().forPath(path) == null) {
        client.create().creatingParentsIfNeeded().forPath(path,data.getBytes());
      }
    }catch(Exception e){
      //throw new exception.
      return false;
    }
    return true;
  }

  public boolean delete(String path){
    try {
      if (client.checkExists().forPath(path) != null) {
        client.delete().forPath(path);
      }
    }catch(Exception e){
      //Todo: Need to throw proper exception.
      return false;
    }
    return true;
  }

  public boolean createLockPath(String path, String child, String data){
    return createIfNotExist(path +"/" + child, data);
  }


  public boolean deleteLockPath(String path, String child) {
    try {
      List<String> children = this.getChildren(path);
      if(children.size() == 0){
        delete(path);
      }else if (children.size() == 1 && children.get(0).equals(child)){
        delete(path + "/" + child);
        delete(path);
      }else if(children.size() > 1){
        delete(path +"/" + child);
        //Check again if the path needs to be deleted.
        if(this.getChildren(path).size() == 0){
          delete(path);
        }
      }else {
        log.error("Lock path corrupted !!!!");
      }
    }catch (Exception e){
      //Todo: Need to throw proper exception.
      return false;
    }
    return true;
  }


  /* package access only */
  public CuratorFramework getClient() {
    return client;
  }

  public String getConnectionString() {
    return connectionString;
  }

  @Override
  public void close() throws Exception {
    stop();
  }
}
