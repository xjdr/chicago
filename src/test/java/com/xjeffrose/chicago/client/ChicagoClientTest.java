package com.xjeffrose.chicago.client;

import com.netflix.curator.test.TestingServer;
import com.xjeffrose.chicago.Chicago;
import java.net.InetSocketAddress;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class ChicagoClientTest {
  static TestingServer testingServer;// = new TestingServer(2190);
  static Chicago chicago;// = new Chicago();
  static ChicagoClient chicagoClientSingle;// = new ChicagoClient(new InetSocketAddress("127.0.0.1", 12000));
  static ChicagoClient chicagoClientDHT;// = new ChicagoClient(new InetSocketAddress("127.0.0.1", 12000));

  @BeforeClass
  static public void setupFixture() throws Exception {
    testingServer = new TestingServer(2181);
    chicago = new Chicago();
    chicago.main(new String[]{});
    chicagoClientSingle = new ChicagoClient(new InetSocketAddress("127.0.0.1", 12000));
    chicagoClientDHT = new ChicagoClient(testingServer.getConnectString());
  }

  @Test
  public void delete() throws Exception {
    assertEquals(true, chicagoClientSingle.delete("key".getBytes()));
  }

  @Test
  public void deleteMany() throws Exception {
    for (int i = 0; i < 200; i++) {
      String _k = "key"+i;
      byte[] key = _k.getBytes();
      String _v = "val" +i;
      byte[] val = _v.getBytes();
      assertEquals(true, chicagoClientDHT.delete(key));
    }  }

  @Test
  public void read() throws Exception {
    assertEquals("val", new String(chicagoClientSingle.read("key".getBytes())));
  }

  @Test
  public void readMany() throws Exception {
    for (int i = 0; i < 200; i++) {
      String _k = "key"+i;
      byte[] key = _k.getBytes();
      String _v = "val" +i;
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
    for (int i = 0; i < 1500; i++) {
      String _k = "key"+i;
      byte[] key = _k.getBytes();
      String _v = "val" +i;
      byte[] val = _v.getBytes();
      assertEquals(true, chicagoClientDHT.write(key, val));
    }
  }


}