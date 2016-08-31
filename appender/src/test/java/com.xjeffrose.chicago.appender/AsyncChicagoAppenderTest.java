package com.xjeffrose.chicago.appender;

import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.Priority;
import org.apache.log4j.RollingFileAppender;
import org.apache.log4j.varia.FallbackErrorHandler;
import org.junit.Test;

/**
 * Created by smadan on 8/22/16.
 */
public class AsyncChicagoAppenderTest {

  @Test
  public void testAppeclientnderNoServer(){
    AsyncChicagoAppender chicagoAppender = new AsyncChicagoAppender();
    chicagoAppender.setChicagoZk("badIP:2181");
    chicagoAppender.setKey("TestKey");
    long start = System.currentTimeMillis();
    try {
      chicagoAppender.activateOptions();
    }catch (Exception e){
      long timeDifference = System.currentTimeMillis() - start;
      assert(timeDifference < 3000);
    }
  }

  //@Test
  public void testActuaLogging() throws InterruptedException {
    Logger log = Logger.getLogger(AsyncChicagoAppenderTest.class.getName());
    try {
      AsyncChicagoAppender chicagoAppender = new AsyncChicagoAppender();
      chicagoAppender.setChicagoZk("10.24.24.235:2181,10.24.23.231:2181,10.24.24.23:2181,10.24.23.230:2181");
      chicagoAppender.setKey("ppfe");
      chicagoAppender.setLayout(new PatternLayout());
      chicagoAppender.setThreshold(Priority.INFO);
      chicagoAppender.activateOptions();
      Logger.getRootLogger().removeAllAppenders();
      Logger.getRootLogger().addAppender(chicagoAppender);
      FallbackErrorHandler fb = new FallbackErrorHandler();
      RollingFileAppender rfa = new RollingFileAppender(new PatternLayout(), "/x/var/log/ppfe/ppfe.log");
      rfa.activateOptions();
      fb.setAppender(rfa);
      chicagoAppender.setErrorHandler(fb);
    } catch (Exception e) {
      log.info("Falling back to default Logger");
    }

    for(int i =0 ;i<100;i++ ){
      try {
        System.out.println("Logging data " + i);
        log.info("some log " + i);
        Thread.sleep(1000);
      }catch (Exception e){
        e.printStackTrace();
      }
    }
  }
}
