package com.xjeffrose.chicago.loadTest;

import com.netflix.curator.test.TestingServer;
import com.xjeffrose.chicago.Chicago;
import com.xjeffrose.chicago.client.ChicagoClient;
import java.net.InetSocketAddress;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ChicagoLoadTest {
  static ChicagoClient chicagoClientDHT;
  static TestingServer testingServer;
  static Chicago chicago1;
  static Chicago chicago2;
  static Chicago chicago3;
  static Chicago chicago4;

  @BeforeClass
  static public void setupFixture() throws Exception {
    testingServer = new TestingServer(2182);
//    chicago1 = new Chicago();
//    chicago1.main(new String[]{"", "src/test/resources/test1.conf"});
//    chicago2 = new Chicago();
//    chicago2.main(new String[]{"", "src/test/resources/test2.conf"});
//    chicago3 = new Chicago();
//    chicago3.main(new String[]{"", "src/test/resources/test3.conf"});
//    chicago4 = new Chicago();
//    chicago4.main(new String[]{"", "src/test/resources/test4.conf"});

    //chicagoClientDHT = new ChicagoClient("10.24.25.188:2181,10.24.25.189:2181,10.25.145.56:2181,10.24.33.123:2181");
    //chicagoClientDHT = new ChicagoClient("10.22.100.183:2181/chicago");
  }

  @Test
  public void writeMany() throws Exception {
    for (int i = 0; i < 100; i++) {
      String _k = "key" + i;
      byte[] key = _k.getBytes();
      String _v = "val" + i;
      byte[] val = _v.getBytes();
      assertEquals(true, chicagoClientDHT.write(key, val));
    }
  }

  @Test
  public void readMany() throws Exception {
    for (int i = 0; i < 100; i++) {
      String _k = "key" + i;
      byte[] key = _k.getBytes();
      String _v = "val" + i;
      byte[] val = _v.getBytes();
      assertEquals(new String(val), new String(chicagoClientDHT.read(key)));
    }
  }

    @Test
    public void deleteMany() throws Exception {
      for (int i = 0; i < 100; i++) {
        String _k = "sm" + i;
        byte[] key = _k.getBytes();
        assertEquals(true, chicagoClientDHT.delete(key));
      }
    }
}
