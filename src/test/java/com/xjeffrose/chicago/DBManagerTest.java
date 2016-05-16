package com.xjeffrose.chicago;

import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.util.List;
import org.junit.Test;
import org.rocksdb.ReadOptions;

import static org.junit.Assert.*;

public class DBManagerTest {
  DBManager dbManager = new DBManager(new ChiConfig(ConfigFactory.parseFile(new File("test.conf"))));

  @Test
  public void singleEntry() throws Exception {
//    byte[] key = new byte[4];
    byte[] key = "Key".getBytes();
    byte[] val = "Valure".getBytes();

    assertTrue(dbManager.write(key, val));
    assertEquals(new String(val), new String(dbManager.read(key)));
    assertTrue(dbManager.delete(key));
  }


  @Test
  public void getKeys() throws Exception {
    List<byte[]> keySet = dbManager.getKeys(new ReadOptions());
    System.out.println(keySet.size());
  }

}