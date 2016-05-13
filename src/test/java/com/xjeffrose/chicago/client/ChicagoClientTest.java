package com.xjeffrose.chicago.client;

import com.xjeffrose.chicago.Chicago;
import java.net.InetSocketAddress;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class ChicagoClientTest {
  Chicago chicago = new Chicago();
  ChicagoClient chicagoClient = new ChicagoClient(new InetSocketAddress("127.0.0.1", 12000));

  @Before
  public void setUp() throws Exception {
    chicago.main(new String[]{"Foo"});
  }

  @Test
  public void delete() throws Exception {
    assertEquals(true, chicagoClient.delete("key".getBytes()));
  }

  @Test
  public void deleteMany() throws Exception {
    for (int i = 0; i < 200; i++) {
      String _k = "key"+i;
      byte[] key = _k.getBytes();
      String _v = "val" +i;
      byte[] val = _v.getBytes();
      assertEquals(true, chicagoClient.delete(key));
    }  }

  @Test
  public void read() throws Exception {
    assertEquals("val", new String(chicagoClient.read("key".getBytes())));
  }

  @Test
  public void readMany() throws Exception {
    for (int i = 0; i < 200; i++) {
      String _k = "key"+i;
      byte[] key = _k.getBytes();
      String _v = "val" +i;
      byte[] val = _v.getBytes();
      assertEquals(new String(val), new String(chicagoClient.read(key)));
    }
  }


  @Test
  public void writeSingle() throws Exception {
    assertEquals(true, chicagoClient.write("key".getBytes(), "val".getBytes()));
  }

  @Test
  public void writeMany() throws Exception {
    for (int i = 0; i < 200; i++) {
      String _k = "key"+i;
      byte[] key = _k.getBytes();
      String _v = "val" +i;
      byte[] val = _v.getBytes();
      assertEquals(true, chicagoClient.write(key, val));
    }
  }


}