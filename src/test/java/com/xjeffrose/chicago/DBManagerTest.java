package com.xjeffrose.chicago;

import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;
import org.rocksdb.BuiltinComparator;
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

  @Test
  public void getOrderedKeys() throws Exception {
    for (int i = 0; i < 20; i++) {
      byte[] k = ("key" + i ).getBytes();
      byte[] v = ("val" + i ).getBytes();
      dbManager.write("foo".getBytes(), k, v);
    }

    List<byte[]> keySet = dbManager.getAllAfter("foo".getBytes(), "key0".getBytes());

    for (int i = 0; i < 20; i++) {
      assertEquals("key" + i , new String(keySet.get(i)));
    }


    }
}