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
  public void read() throws Exception {
    assertEquals("val", new String(chicagoClient.read("key".getBytes())));
  }


  @Test
  public void write() throws Exception {
    assertEquals(true, chicagoClient.write("key".getBytes(), "val".getBytes()));
  }


}