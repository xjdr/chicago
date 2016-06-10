package com.xjeffrose.chicago;

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
    mapping.put("zk_hosts", zk_hosts);
    mapping.put("db_path", db_filename.getPath());
    mapping.put("workers", 20);
    mapping.put("quorum", 3);
    mapping.put("boss_count", 4);
    mapping.put("admin_bind_ip", "127.0.0.1");
    if(ports){
      mapping.put("admin_port", 9000+server_num);
      mapping.put("stats_port", 8000+server_num);
      mapping.put("db_port", 12000+server_num);
    }else{
      mapping.put("admin_port",0);
      mapping.put("stats_port",0);
      mapping.put("db_port", 0);
    }
    mapping.put("stats_bind_ip", "127.0.0.1");
    mapping.put("db_bind_ip", "127.0.0.1");
    mapping.put("X509_CERT", "certs/cert.pem");
    mapping.put("PRIVATE_KEY", "certs/privateKey.pem");
    mapping.put("compaction_size", 60);
    mapping.put("database_mode", false);
  
    return new ChiConfig(ConfigFactory.parseMap(mapping));
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
      servers.put("chicago"+i,new ChicagoServer(makeConfig(tmp, i, zk_hosts,true)));
    }
    return servers;
  }
}
