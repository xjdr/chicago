package com.xjeffrose.chicago.server;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString
public class ChiConfig {
  @Getter
  private final String zkHosts;
  private Config conf;
  //  private Map<XioServerDef, XioMetrics> channelStats;
  @Getter
  private String dbPath;
  @Getter
  private int quorum;
  @Getter
  private boolean graceFullStart;
  @Getter
  private long compactionSize;
  @Getter
  private boolean databaseMode;
  @Getter
  private boolean encryptAtRest;
  @Getter
  private List<String> witnessList;
//  private ZkClient zkClient;

  public ChiConfig(Config conf) {

    this.conf = conf;
    Config defaults = ConfigFactory.parseString("graceful = false");

    this.dbPath = conf.getString("settings.dbPath");
    this.quorum = conf.getInt("settings.quorum");
    this.zkHosts = conf.getString("settings.zookeeperCluster");
    this.graceFullStart = conf.withFallback(defaults).getBoolean("graceful");
    this.compactionSize = conf.getMemorySize("settings.compactionSize").toBytes();
    this.databaseMode = conf.getBoolean("settings.databaseMode");
    this.witnessList = conf.getStringList("settings.witnessList");
    this.encryptAtRest =  conf.getBoolean("settings.encryptAtRest");
  }

  public String getZkHosts() {
    return zkHosts;
  }

  public Config getConf() {
    return conf;
  }

  public String getDbPath() {
    return dbPath;
  }

  public int getQuorum() {
    return quorum;
  }

  public boolean isGraceFullStart() {
    return graceFullStart;
  }

  public long getCompactionSize() {
    return compactionSize;
  }

  public boolean isDatabaseMode() {
    return databaseMode;
  }

  public List<String> getWitnessList() {
    return witnessList;
  }

  /*
  public void setChannelStats(Map<XioServerDef, XioMetrics> channelStats) {
    this.channelStats = channelStats;
  }
  */
}
