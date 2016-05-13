package com.xjeffrose.chicago;

import com.typesafe.config.ConfigFactory;
import java.io.File;
import org.junit.Test;

import static org.junit.Assert.*;

public class DBManagerTest {
  DBManager dbManager = new DBManager(new ChiConfig(ConfigFactory.parseFile(new File("test.conf"))));

  @Test
  public void singleEntry() throws Exception {
//    byte[] key = new byte[4];
    byte[] key = "Key".getBytes();
    byte[] val = "Valure".getBytes();

    assertTrue(dbManager.write(key, val));
    assertEquals(new String(val), new String(dbManager.get(key)));
    assertTrue(dbManager.delete(key));
  }

}