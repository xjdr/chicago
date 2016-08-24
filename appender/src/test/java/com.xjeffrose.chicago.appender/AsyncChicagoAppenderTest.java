package com.xjeffrose.chicago.appender;

import org.junit.Test;

/**
 * Created by smadan on 8/22/16.
 */
public class AsyncChicagoAppenderTest {

  //@Test
  public void testAppenderNoServer(){
    AsyncChicagoAppender chicagoAppender = new AsyncChicagoAppender();
    chicagoAppender.setChicagoZk("junkIP:2181");
    chicagoAppender.setKey("TestKey");
    try {
      chicagoAppender.activateOptions();
    }catch (Exception e){
      e.printStackTrace();
    }
  }
}
