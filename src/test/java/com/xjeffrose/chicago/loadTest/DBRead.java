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

public class DBRead {
  public static void main(String args[]) throws Exception{
    ChicagoClient cc = new ChicagoClient(new InetSocketAddress("10.24.25.188", 12000));
    String k = "sm0";
    System.out.println("Found value in 10.24.25.188" + new String(cc.read(k.getBytes()).get()));

    cc = new ChicagoClient(new InetSocketAddress("10.25.145.56", 12000));
    k = "sm0";
    System.out.println("Found value in 10.25.145.56 " + new String(cc.read(k.getBytes()).get()));

    cc = new ChicagoClient(new InetSocketAddress("10.24.25.189", 12000));
    k = "sm0";
    System.out.println("Found value in 10.24.25.189 " + new String(cc.read(k.getBytes()).get()));

    cc = new ChicagoClient(new InetSocketAddress("10.24.33.123", 12000));
    k = "sm0";
    System.out.println("Found value in 10.24.33.123 " + new String(cc.read(k.getBytes()).get()));

    System.exit(-1);

  }

}
