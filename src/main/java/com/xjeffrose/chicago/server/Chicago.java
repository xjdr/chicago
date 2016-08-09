package com.xjeffrose.chicago.server;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Chicago {
  private static final Logger log = LoggerFactory.getLogger(Chicago.class.getName());

  public static void main(String[] args) {
    log.info("Starting Chicago, have a nice day");
/*
    Config overrides;

    if (args.length > 0) {
      try {
        overrides = ConfigFactory.parseFile(new File(args[0]));
      } catch (Exception e) {
        overrides = ConfigFactory.parseFile(new File("application.conf"));
      }
    } else {
      overrides = ConfigFactory.parseFile(new File("test.conf"));
    }

    Config defaults = ConfigFactory.load();

    System.out.println(overrides);
    Config settings = overrides.withFallback(defaults).resolve();
    */
    Config settings = ConfigFactory.load();
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
