package com.xjeffrose.chicago;

import com.xjeffrose.chicago.server.ChicagoServer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Chicago {
  private static final Logger log = LoggerFactory.getLogger(Chicago.class.getName());

  public static void main(String[] args) {
    log.info("Starting Chicago, have a nice day");

    Config _conf;

    if (args.length > 0) {
      try {
        _conf = ConfigFactory.parseFile(new File(args[1]));
      } catch (Exception e) {
        _conf = ConfigFactory.parseFile(new File("application.conf"));
      }
    } else {
      _conf = ConfigFactory.parseFile(new File("test.conf"));
    }

    ChiConfig config = new ChiConfig(_conf);

    try {
      ChicagoServer server = new ChicagoServer(config);
      server.start();
    } catch (Exception e) {
      log.error("Error Starting Chicago", e);
      throw new RuntimeException(e);
    }
  }
}
