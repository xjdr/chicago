package com.xjeffrose.chicago.client;

import com.google.common.collect.Lists;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.*;

public class RendezvousHashTest {
  private static final Funnel<CharSequence> strFunnel = Funnels.stringFunnel(Charset.defaultCharset());


  @Test
  public void getTest() throws Exception {
    List<String> nodes = Lists.newArrayList();
    for (int i = 0; i < 12; i++) {
      nodes.add("node" + i);
    }
    RendezvousHash rendezvousHash1 = new RendezvousHash(strFunnel, nodes, 3);
    RendezvousHash rendezvousHash2 = new RendezvousHash(strFunnel, nodes, 3);

    for (int i = 0; i < 10; i++) {
      byte[] x = ("key" + i).getBytes();
      assertEquals(rendezvousHash1.get(x), rendezvousHash2.get(x));
//      assertEquals(rendezvousHash2.get(x), rendezvousHash2.get(x));
    }
  }

  @Test
  public void getListTest() throws Exception {
    for (int d = 0; d < 20000; d++) {
      List<String> nodes = Lists.newArrayList();
      for (int i = 0; i < 12; i++) {
        nodes.add("node" + i);
      }
      RendezvousHash rendezvousHash1 = new RendezvousHash(strFunnel, nodes, 3);
      RendezvousHash rendezvousHash2 = new RendezvousHash(strFunnel, nodes, 3);

      for (int i = 0; i < 10; i++) {
        byte[] x = ("key" + i).getBytes();
        assertEquals(rendezvousHash1.get(x), rendezvousHash2.get(x));
//      assertEquals(rendezvousHash2.getOld(x), rendezvousHash2.getList(x).get(0));
      }

      List<String> xx = rendezvousHash2.get("key1".getBytes());

      rendezvousHash1.remove("node11");
      rendezvousHash2.remove("node11");
      assertEquals(rendezvousHash1.get("key1".getBytes()), rendezvousHash2.get("key1".getBytes()));
      assertEquals(rendezvousHash2.get("key1".getBytes()).get(0), xx.get(1));

      rendezvousHash1.add("node11");
      rendezvousHash2.add("node11");
      rendezvousHash1.remove("node1");
      rendezvousHash2.remove("node1");

      assertEquals(rendezvousHash1.get("key1".getBytes()), rendezvousHash2.get("key1".getBytes()));
      assertEquals(rendezvousHash2.get("key1".getBytes()), xx);
    }
  }

  @Test
  public void getChrisListTest() throws Exception {
    for (int d = 0; d < 20000; d++) {
      List<String> nodes = Lists.newArrayList();
      for (int i = 0; i < 12; i++) {
        nodes.add("node" + i);
      }
      RendezvousHash rendezvousHash1 = new RendezvousHash(strFunnel, nodes, 3);
      RendezvousHash rendezvousHash2 = new RendezvousHash(strFunnel, nodes, 3);

      for (int i = 0; i < 10; i++) {
        byte[] x = ("key" + i).getBytes();
        assertEquals(rendezvousHash1.this_is_why_i_pay_chris(x), rendezvousHash2.this_is_why_i_pay_chris(x));
//      assertEquals(rendezvousHash2.getOld(x), rendezvousHash2.getList(x).get(0));
      }

      List<String> xx = rendezvousHash2.this_is_why_i_pay_chris("key1".getBytes());

      rendezvousHash1.remove("node11");
      rendezvousHash2.remove("node11");
      assertEquals(rendezvousHash1.this_is_why_i_pay_chris("key1".getBytes()), rendezvousHash2.this_is_why_i_pay_chris("key1".getBytes()));
      assertEquals(rendezvousHash2.this_is_why_i_pay_chris("key1".getBytes()).get(0), xx.get(1));

      rendezvousHash1.add("node11");
      rendezvousHash2.add("node11");
      rendezvousHash1.remove("node1");
      rendezvousHash2.remove("node1");

      assertEquals(rendezvousHash1.this_is_why_i_pay_chris("key1".getBytes()), rendezvousHash2.this_is_why_i_pay_chris("key1".getBytes()));
      assertEquals(rendezvousHash2.this_is_why_i_pay_chris("key1".getBytes()), xx);

    }
  }


  @Test
  public void getMany() throws Exception {
    List<String> nodes = Lists.newArrayList();
    for(int i = 0 ; i < 200; i ++) {
      nodes.add("node"+i);
    }
    RendezvousHash rendezvousHash1 = new RendezvousHash(strFunnel , nodes, 3);
    RendezvousHash rendezvousHash2 = new RendezvousHash(strFunnel , nodes, 3);

    int loopCount = 0;

    for (int i = 0; i < 10000; i++) {
      byte[] x = ("key" + i).getBytes();
      assertEquals(rendezvousHash1.get(x), rendezvousHash2.get(x));
      if (i == 9999) {
        i = 0;
        loopCount++;
      }

      if (loopCount == 2) {
        break;
      }
    }

    rendezvousHash1.remove("node3");
    rendezvousHash2.remove("node3");

    loopCount = 0;

    for (int i = 0; i < 10000; i++) {
      byte[] x = ("key" + i).getBytes();
      assertEquals(rendezvousHash1.get(x), rendezvousHash2.get(x));
      if (i == 9999) {
        i = 0;
        loopCount++;
      }

      if (loopCount == 2) {
        break;
      }
    }

    rendezvousHash1.add("node3");
    rendezvousHash2.add("node3");

    loopCount = 0;

    for (int i = 0; i < 10000; i++) {
      byte[] x = ("key" + i).getBytes();
      assertEquals(rendezvousHash1.get(x), rendezvousHash2.get(x));
      if (i == 9999) {
        i = 0;
        loopCount++;
      }

      if (loopCount == 2) {
        break;
      }
    }
  }

  @Test
  public void testOrder() {
    String key="key";
    ArrayList<String> nodes = new ArrayList<>();
    nodes.add("60");
    nodes.add("10");
    nodes.add("20");
    nodes.add("50");
    nodes.add("10");
    nodes.add("40");
    nodes.add("90");
    nodes.add("33");

    RendezvousHash rendezvousHash = new RendezvousHash(
      Funnels.stringFunnel(Charset.defaultCharset()), nodes, 3);
    printHash(rendezvousHash,key);

    //Adding 100 series node
    rendezvousHash.add("160");
    printHash(rendezvousHash,key);

    //Adding more nodes in 200 series.
    rendezvousHash.add("260");
    rendezvousHash.add("220");
    rendezvousHash.add("299");
    rendezvousHash.add("255");
    printHash(rendezvousHash,key);

    //Adding 300 series
    for(int i =0;i<50;i++) {
      rendezvousHash.add(Integer.toString(300+i));
    }
    printHash(rendezvousHash,key);
    List<String> n4 = rendezvousHash.get(key.getBytes());
  }

  public void printHash(RendezvousHash r, String key){
    System.out.println("Hash value :");
    r.get(key.getBytes()).forEach(x -> {
      System.out.print(x + " ");
    });
    System.out.println();
  }

}
