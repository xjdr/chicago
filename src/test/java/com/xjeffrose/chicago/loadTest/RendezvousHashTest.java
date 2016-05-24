package com.xjeffrose.chicago.loadTest;

import com.google.common.hash.Funnels;
import com.xjeffrose.chicago.client.ChicagoClient;
import com.xjeffrose.chicago.client.RendezvousHash;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

/**
 * Created by root on 5/18/16.
 */
public class RendezvousHashTest {

  @Test
  public void testOrder() {
    ArrayList<String> nodes = new ArrayList<>();
    nodes.add("60");
    nodes.add("10");
    nodes.add("20");
    nodes.add("50");
    nodes.add("30");
    nodes.add("40");

    RendezvousHash rendezvousHash = new RendezvousHash(
        Funnels.stringFunnel(Charset.defaultCharset()), nodes);

    for (int i = 0; i < 1; i++) {
      String _k = "key" + i;
      List<String> l =  rendezvousHash.get(_k.getBytes());
      System.out.println("key = "+ _k);
      l.forEach(x -> {
        System.out.println(x);
      });
    }
  }

  @Test
  public void testGetNodeList() throws Exception{
    ChicagoClient chicagoClient = new ChicagoClient("10.24.25.188:2181,10.24.25.189:2181,10.25.145.56:2181,10.24.33.123:2181");
    String k = "Key";
    while(true) {
      System.out.println((chicagoClient.getNodeList(k.getBytes())).toString());
      Thread.sleep(500);
    }
  }
}
