package com.xjeffrose.chicago;

import com.typesafe.config.Config;
import com.xjeffrose.chicago.server.ChicagoServer;
import java.io.File;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import com.typesafe.config.ConfigFactory;
import org.junit.rules.TemporaryFolder;

/**
 * Utility class for testing
 */
public class TestChicago {

  public static ChiConfig makeConfig(File tmp_dir, int server_num, String zk_hosts, boolean ports) {
    File db_filename = new File(tmp_dir, "test" + server_num + ".db");

    Map<String, Object> mapping = new HashMap<>();
    mapping.put("settings.zookeeperCluster", zk_hosts);
    mapping.put("settings.dbPath", db_filename.getPath());
    if(ports){
      mapping.put("settings.admin.bindPort", 9000+server_num);
      mapping.put("settings.stats.bindPort", 8000+server_num);
      mapping.put("settings.db.bindPort", 12000+server_num);
      mapping.put("settings.election.bindPort", 12001+server_num);
      mapping.put("settings.rpc.bindPort", 12002+server_num);
    }
    mapping.put("witness_list", "127.0.0.1:120010, 127.0.0.1:120011, 127.0.0.1:120012");

    Config defaults = ConfigFactory.load().getConfig("chicago.application");
    Config overrides = ConfigFactory.parseMap(mapping);
    return new ChiConfig(overrides.withFallback(defaults));
  }

  public static File chicago_dir(TemporaryFolder tmp) {
    try {
      return tmp.newFolder("chicago");
    } catch (java.io.IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static List<ChicagoServer> makeServers(File tmp, int count, String zk_hosts) {
    List<ChicagoServer> servers = new ArrayList<ChicagoServer>();
    for (int i = 1; i <= count; i++) {
      servers.add(new ChicagoServer(makeConfig(tmp, i, zk_hosts, false)));
    }
    return servers;
  }

  public static HashMap<String,ChicagoServer> makeNamedServers(File tmp, int count, String zk_hosts) {
    HashMap<String,ChicagoServer> servers = new HashMap<>();
    for (int i = 1; i <= count; i++) {
      servers.put("chicago"+i,new ChicagoServer(makeConfig(tmp, i*5, zk_hosts,true)));
    }
    return servers;
  }
}
