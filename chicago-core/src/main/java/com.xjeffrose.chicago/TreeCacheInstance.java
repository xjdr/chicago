package com.xjeffrose.chicago;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TreeCacheInstance {
  private static final Logger log = LoggerFactory.getLogger(TreeCacheInstance.class.getName());
  private final ZkClient zkClient;
  private final CuratorFramework client;
  private final String root;
  private final TreeCache cache;

  public TreeCacheInstance(ZkClient zkClient, String root) {
    this.zkClient = zkClient;
    this.client = zkClient.getClient();
    this.root = root;

    cache = TreeCache.newBuilder(this.client,this.root).build();
  }

  public void start() throws Exception {
    cache.start();
  }

  public void stop() {
    CloseableUtils.closeQuietly(cache);
    //CloseableUtils.closeQuietly(client);
  }

  public void close() {
    CloseableUtils.closeQuietly(cache);
    CloseableUtils.closeQuietly(client);
  }

  public TreeCache getCache() {
    return cache;
  }

  public String getRoot() {
    return root;
  }
}
