package com.xjeffrose.chicago;

import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;
import org.rocksdb.ReadOptions;

import static org.junit.Assert.*;

public class DBManagerTest {
  static DBManager dbManager;

  @BeforeClass
  static public void setupFixture() throws Exception {
    dbManager = new DBManager(new ChiConfig(ConfigFactory.parseFile(new File("test.conf"))));
  }

  @Test
  public void singleEntry() throws Exception {
    byte[] colFam = "ColFam".getBytes();
    byte[] key = "Key".getBytes();
    byte[] val = "Valure".getBytes();

    assertTrue(dbManager.write(colFam, key, val));
    assertEquals(new String(val), new String(dbManager.read(colFam, key)));
    assertTrue(dbManager.delete(colFam, key));
  }


  @Test
  public void getKeys() throws Exception {
    List<byte[]> keySet = dbManager.getKeys(new ReadOptions());
  }

}