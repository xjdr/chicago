package com.xjeffrose.chicago.loadTest;

import com.google.common.hash.Funnels;
import com.xjeffrose.chicago.client.ChicagoClient;
import com.xjeffrose.chicago.client.RendezvousHash;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Created by root on 5/18/16.
 */
public class RendezvousHashTest {
    static ChicagoClient chicagoClientDHT;

    @BeforeClass
    static public void setupFixture() throws Exception{
        chicagoClientDHT = new ChicagoClient("10.24.25.188:2181,10.24.25.189:2181,10.25.145.56:2181,10.24.33.123:2181");
        //chicagoClientDHT =  new ChicagoClient("10.22.100.183:2181,10.25.180.234:2181,10.22.103.86:2181,10.25.180.247:2181,10.25.69.226:2181/chicago");
        //chicagoClientDHT = new ChicagoClient("10.22.100.183:2181/chicago");
    }

    @Test
    public void TestBadNode() throws Exception{
        while(true){
            String key = "key";
            List<String> l = chicagoClientDHT.getNodeListforKey(key.getBytes());
            System.out.println(l.toString());
            Thread.sleep(100);
        }
    }
    @Test
    public void testHash(){
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
}
