package com.xjeffrose.chicago;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
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
    dbManager = new DBManager(TestChicago.makeConfig(TestChicago.chicago_dir(tmp), 1, "",false), null);
  }

  @Test
  public void TestDatFormat(){
    byte[] colFam = new String("chicago").getBytes();
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    Date date = new Date();
    String time = dateFormat.format(date);
    String key = new String(colFam).concat("-").concat(time);
    System.out.print(key);
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
