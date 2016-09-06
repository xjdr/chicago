package com.xjeffrose.chicago.server;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by smadan on 8/29/16.
 */
public class ChicagoCluster {
  private static final Logger log = LoggerFactory.getLogger(Chicago.class.getName());

  public static void main(String[] args) {
    log.info("Starting Chicago, have a nice day");
    for (int i = 1; i < 5; i++) {
      Config settings = ConfigFactory.load("app"+i+".conf");
      ChiConfig config = new ChiConfig(settings.getConfig("chicago.application"));

      try {
        ChicagoServer server = new ChicagoServer(config);
        server.start();
      } catch (Exception e) {
        log.error("Error Starting Chicago", e);
        throw new RuntimeException(e);
      }
    }
  }
}
