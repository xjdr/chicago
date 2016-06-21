package com.xjeffrose.chicago;

import org.junit.Test;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;

public class DBManagerTest {
  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();
  DBManager dbManager;

  @Before
  public void setupFixture() throws Exception {
    dbManager = new DBManager(TestChicago.makeConfig(TestChicago.chicago_dir(tmp), 1, "",false));
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
  public void tsEntry() throws Exception {
    byte[] offset = null;

    for (int i = 0; i < 20; i++) {
      if (i == 12) {
        offset = dbManager.tsWrite("key".getBytes(), Integer.toString(i).getBytes());
      } else {
        dbManager.tsWrite("key".getBytes(), Integer.toString(i).getBytes());
      }
    }


    assertNotNull(dbManager.stream("key".getBytes()));

    assertNotNull(dbManager.stream("key".getBytes(), offset));

  }

}
