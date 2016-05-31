package com.xjeffrose.chicago;

import com.typesafe.config.Config;
import com.xjeffrose.xio.core.XioMetrics;
import com.xjeffrose.xio.server.XioServerDef;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChiConfig {
  private static final Logger log = LoggerFactory.getLogger(ChiConfig.class.getName());
  private final int dbPort;
  private final String zkHosts;
  private String dbBindIP;
  private String X509_CERT;
  private String PRIVATE_KEY;
  private Config conf;
  private DBRouter dbRouter;
  private int workers;
  private Map<XioServerDef, XioMetrics> channelStats;
  private int bossCount;
  private String adminBindIP;
  private int adminPort;
  private int statsPort;
  private String statsBindIP;
  private int DBPort;
  private String DBBindIP;
  private String cert;
  private String key;
  private String dbPath;
  private LeaderSelector leaderSelector;
  private ZkClient zkClient;
  private int quorum;

  public ChiConfig(Config conf) {

    this.conf = conf;
    try {
      this.X509_CERT = new String(Files.readAllBytes(Paths.get(conf.getString("X509_CERT")).toAbsolutePath()));
      this.PRIVATE_KEY = new String(Files.readAllBytes(Paths.get(conf.getString("PRIVATE_KEY")).toAbsolutePath()));
    } catch (IOException e) {
      this.X509_CERT = null;
      this.PRIVATE_KEY = null;
      e.printStackTrace();
    }

    this.dbPath = conf.getString("db_path");
    this.workers = conf.getInt("workers");
    this.bossCount =  conf.getInt("boss_count");
    this.adminBindIP = conf.getString("admin_bind_ip");
    this.adminPort = conf.getInt("admin_port");
    this.statsBindIP = conf.getString("stats_bind_ip");
    this.statsPort = conf.getInt("stats_port");
    this.dbBindIP = conf.getString("db_bind_ip");
    this.dbPort = conf.getInt("db_port");
    this.quorum = conf.getInt("quorum");
    this.zkHosts = conf.getString("zk_hosts");

  }

  public void setDbRouter(DBRouter dbRouter) {
    this.dbRouter = dbRouter;
  }

  public int getWorkers() {
    return workers;
  }

  public void setChannelStats(Map<XioServerDef, XioMetrics> channelStats) {
    this.channelStats = channelStats;
  }

  public int getBossCount() {
    return bossCount;
  }

  public String getAdminBindIP() {
    return adminBindIP;
  }

  public int getAdminPort() {
    return adminPort;
  }

  public int getStatsPort() {
    return statsPort;
  }

  public String getStatsBindIP() {
    return statsBindIP;
  }

  public int getDBPort() {
    return dbPort;
  }

  public String getDBBindIP() {
    return dbBindIP;
  }

  public String getDBBindEndpoint() {
    return dbBindIP + ":" + dbPort;
  }

  public String getCert() {
    return X509_CERT;
  }

  public String getKey() {
    return PRIVATE_KEY;
  }

  public String getDBPath() {
    return dbPath;
  }

  public String getZkHosts() {
    return zkHosts;
  }

  public void setLeaderSelector(LeaderSelector leaderSelector) {
    this.leaderSelector = leaderSelector;
  }

  public void setZkClient(ZkClient zkClient) {
    this.zkClient = zkClient;
  }

  public LeaderSelector getLeaderSelector() {
    return leaderSelector;
  }

  public ZkClient getZkClient() {
    return zkClient;
  }

 public int getQuorum() {
   return quorum;
 }

}
