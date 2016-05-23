package com.xjeffrose.chicago;

import java.nio.charset.Charset;
import java.util.List;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;

public class ZkClient {
  private static final Logger log = Logger.getLogger(ZkClient.class.getName());

  private CuratorFramework client;
  private String connectionString;

  public ZkClient(CuratorFramework client) {
    this.client = client;
    this.connectionString = client.getZookeeperClient().getCurrentConnectionString();
  }

  public ZkClient(String serverSet) {
    connectionString = serverSet;
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    client = CuratorFrameworkFactory.newClient(serverSet, retryPolicy);
  }

  public void rebuild() {
    client.close();
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 4);
    client = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
  }

  public void stop(){
    client.close();
  }

  public void start() throws InterruptedException {
    client.start();
    client.blockUntilConnected();
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
      return client.getChildren().forPath(path);
    } catch (Exception e) {
      //throw new RuntimeException(e);
    }
    return null;
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

  /* package access only */
  public CuratorFramework getClient() {
    return client;
  }

  public String getConnectionString() {
    return connectionString;
  }
}

