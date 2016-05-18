package com.xjeffrose.chicago.loadTest;


import com.google.common.hash.Funnels;
import com.xjeffrose.chicago.client.ChicagoClient;
import com.xjeffrose.chicago.client.RendezvousHash;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import org.apache.log4j.Logger;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

/**
 * Created by root on 5/17/16.
 */
public class DBRead {

  public static void main(String args[]) throws Exception{
    //ChicagoClient cc = new ChicagoClient(new InetSocketAddress("10.25.69.226", 12000));
    //String k = "key"+3;
    //System.out.println(new String(cc.read(k.getBytes())));
    //System.exit(-1);
    ArrayList<String> nodes = new ArrayList<>();
    nodes.add("10.24.25.123");
    nodes.add("10.24.25.189");
    nodes.add("10.25.145.56");
    nodes.add("10.24.33.188");
    RendezvousHash rendezvousHash = new RendezvousHash(
        Funnels.stringFunnel(Charset.defaultCharset()), nodes);

    for (int i = 0; i < 500; i++) {
      String _k = "key" + i;
      System.out.println("key = "+ _k +" , "+(String) rendezvousHash.get(_k.getBytes()));
    }

  }

}
