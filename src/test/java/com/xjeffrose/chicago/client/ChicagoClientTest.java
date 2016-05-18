package com.xjeffrose.chicago.client;

import com.netflix.curator.test.TestingServer;
import com.xjeffrose.chicago.Chicago;
import java.net.InetSocketAddress;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ChicagoClientTest {
  static TestingServer testingServer;// = new TestingServer(2190);
  static Chicago chicago1;// = new Chicago();
  static Chicago chicago2;// = new Chicago();
  static Chicago chicago3;// = new Chicago();
  static Chicago chicago4;// = new Chicago();

  static ChicagoClient chicagoClientSingle;// = new ChicagoClient(new InetSocketAddress("127.0.0.1", 12000));
  static ChicagoClient chicagoClientDHT;// = new ChicagoClient(new InetSocketAddress("127.0.0.1", 12000));

  @BeforeClass
  static public void setupFixture() throws Exception {
    testingServer = new TestingServer(2182);
    chicago1 = new Chicago();
    chicago1.main(new String[]{"", "src/test/resources/test1.conf"});
    chicago2 = new Chicago();
    chicago2.main(new String[]{"", "src/test/resources/test2.conf"});
    chicago3 = new Chicago();
    chicago3.main(new String[]{"", "src/test/resources/test3.conf"});
    chicago4 = new Chicago();
    chicago4.main(new String[]{"", "src/test/resources/test4.conf"});
    chicagoClientSingle = new ChicagoClient(new InetSocketAddress("127.0.0.1", 12000));
//    chicagoClientDHT = new ChicagoClient("10.25.160.234:2181");
//    chicagoClientDHT = new ChicagoClient(testingServer.getConnectString());
    chicagoClientDHT = new ChicagoClient("10.24.25.188:2181,10.24.25.189:2181,10.25.145.56:2181,10.24.33.123:2181");

  }

  @Test
  public void delete() throws Exception {
    assertEquals(true, chicagoClientSingle.delete("key".getBytes()));
  }

  @Test
  public void deleteMany() throws Exception {
    for (int i = 0; i < 20000; i++) {
      String _k = "key" + i;
      byte[] key = _k.getBytes();
      String _v = "val" + i;
      byte[] val = _v.getBytes();
      assertEquals(true, chicagoClientDHT.delete(key));
    }
  }

  @Test
  public void read() throws Exception {
    assertEquals("val", new String(chicagoClientSingle.read("key".getBytes())));
  }

  @Test
  public void readMany() throws Exception {
    for (int i = 0; i < 20000; i++) {
      String _k = "key" + i;
      byte[] key = _k.getBytes();
      String _v = "val" + i;
      byte[] val = _v.getBytes();
      assertEquals(new String(val), new String(chicagoClientDHT.read(key)));
    }
  }


  @Test
  public void writeSingle() throws Exception {
    assertEquals(true, chicagoClientSingle.write("key".getBytes(), "val".getBytes()));
  }

  @Test
  public void writeMany() throws Exception {
    for (int i = 0; i < 20; i++) {
      String _k = "key" + i;
      byte[] key = _k.getBytes();
      String _v = "val" + i;
      byte[] val = _v.getBytes();
      boolean resp = chicagoClientDHT.write(key, val);
//      System.out.println(resp);
      assertEquals(true, resp);
    }
  }
}