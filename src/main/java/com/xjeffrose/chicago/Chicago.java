package com.xjeffrose.chicago;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import kafka.producer.KafkaLog4jAppender;
import org.apache.log4j.Logger;

public class Chicago {

  private static final Logger log = Logger.getLogger(Chicago.class.getName());

  public static void main(String[] args) {

    //Setting the Log topic for Kafka
    if(System.getProperty("ENV") != null && Logger.getRootLogger().getAppender("KAFKA") != null) {
      KafkaLog4jAppender kafka = (KafkaLog4jAppender) Logger.getRootLogger().getAppender("KAFKA");
      String env = System.getProperty("ENV");

      kafka.setTopic(kafka.getTopic() + "-" + env);

    }

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
    DBRouter dbRouter = new DBRouter(config);

    try {
      dbRouter.run();
    } catch (Exception e) {
      System.exit(-1);
    }
  }


}
