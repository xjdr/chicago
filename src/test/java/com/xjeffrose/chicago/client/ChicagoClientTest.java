package com.xjeffrose.chicago.client;

import com.google.common.util.concurrent.SettableFuture;
import com.xjeffrose.chicago.ChicagoMessage;
import com.xjeffrose.chicago.ChicagoObjectDecoder;
import com.xjeffrose.chicago.ChicagoObjectEncoder;
import com.xjeffrose.chicago.Op;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.internal.PlatformDependent;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ChicagoClientTest {
  private final static String NODE_LIST_PATH = "/chicago/node-list";
  private EmbeddedChannel ch1 = new EmbeddedChannel(new ChicagoClientCodec());
  private EmbeddedChannel ch2 = new EmbeddedChannel();
  private EmbeddedChannel ch3 = new EmbeddedChannel();

  private ChicagoObjectDecoder decoder = new ChicagoObjectDecoder();
  private ChicagoObjectEncoder encoder = new ChicagoObjectEncoder();

  private TestingServer zkServer;
  private CuratorFramework curZkClient;
  private ChicagoClient chicagoClient;

  @Before
  public void setUp() throws Exception {
//    try (TestingServer zkServer = new TestingServer(true)) {
//      zkServer.start();
//      try (CuratorFramework curZkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), new ExponentialBackoffRetry(5000, 3))) {
//        curZkClient.start();
//
//        this.zkServer = zkServer;
//        this.curZkClient = curZkClient;
//
//        Map<UUID, SettableFuture<byte[]>> futureMap = PlatformDependent.newConcurrentHashMap();
        List<EmbeddedChannel> hostList = new ArrayList<EmbeddedChannel>();
        hostList.add(ch1);

        try (ChicagoClient chicagoClient = new ChicagoClient(hostList, 1)) {
          this.chicagoClient = chicagoClient;
        }
//      }
//    }
  }

  @After
  public void tearDown() throws Exception {
//    zkServer.stop();
//    curZkClient.close();
    chicagoClient.close();
  }

//  private void register(String path) {
//    try {
//      curZkClient
//          .create()
//          .creatingParentsIfNeeded()
//          .withMode(CreateMode.EPHEMERAL)
//          .forPath(path, null);
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//  }

  @Test
  public void aggregatedStream() throws Exception {
    //TODO(JR): Requires more complex test. Should be broken out into a seperate class.
  }

  @Test
  public void stream() throws Exception {
    chicagoClient.stream("Key".getBytes(), "Offset".getBytes());

    ByteBuf bb = ch1.readOutbound();
    byte[] _bb = new byte[bb.readableBytes()];
    bb.readBytes(_bb);
    ChicagoMessage chicagoMessage = decoder.decode(_bb);

    assertEquals(Op.STREAM, chicagoMessage.getOp());
    assertEquals("Key", new String(chicagoMessage.getColFam()));
    assertEquals("Offset", new String(chicagoMessage.getVal()));
  }

  @Test
  public void read() throws Exception {
    chicagoClient.read("ColFam".getBytes(), "Key".getBytes());

    ByteBuf bb = ch1.readOutbound();
    byte[] _bb = new byte[bb.readableBytes()];
    bb.readBytes(_bb);
    ChicagoMessage chicagoMessage = decoder.decode(_bb);

    assertEquals(Op.READ, chicagoMessage.getOp());
    assertEquals("ColFam", new String(chicagoMessage.getColFam()));
    assertEquals("Key", new String(chicagoMessage.getKey()));

  }

  @Test
  public void write() throws Exception {
    chicagoClient.write("ColFam".getBytes(), "Key".getBytes(), "Val".getBytes());

    ByteBuf bb = ch1.readOutbound();
    byte[] _bb = new byte[bb.readableBytes()];
    bb.readBytes(_bb);
    ChicagoMessage chicagoMessage = decoder.decode(_bb);

    assertEquals(Op.WRITE, chicagoMessage.getOp());
    assertEquals("ColFam", new String(chicagoMessage.getColFam()));
    assertEquals("Key", new String(chicagoMessage.getKey()));
    assertEquals("Val", new String(chicagoMessage.getVal()));

  }


  @Test
  public void tsWrite() throws Exception {
    chicagoClient.tsWrite("ColFam".getBytes(), "Val".getBytes());

    ByteBuf bb = ch1.readOutbound();
    byte[] _bb = new byte[bb.readableBytes()];
    bb.readBytes(_bb);
    ChicagoMessage chicagoMessage = decoder.decode(_bb);

    assertEquals(Op.TS_WRITE, chicagoMessage.getOp());
    assertEquals("ColFam", new String(chicagoMessage.getColFam()));
    assertEquals("Val", new String(chicagoMessage.getVal()));
  }

  @Test
  public void tsbatchWrite() throws Exception {
    for (int i = 0; i < 10000; i++) {
      String key = "Key";
      String val = "Val" + i;
      chicagoClient.tsbatchWrite(key.getBytes(), val.getBytes());
    }

    ByteBuf bb = ch1.readOutbound();
    byte[] _bb = new byte[bb.readableBytes()];
    bb.readBytes(_bb);
    ChicagoMessage chicagoMessage = decoder.decode(_bb);

    assertEquals(Op.TS_WRITE, chicagoMessage.getOp());
    assertEquals("Key", new String(chicagoMessage.getColFam()));
    assertTrue(new String(chicagoMessage.getVal()).contains("Val0"));
  }

  @Test
  public void delete() throws Exception {
    chicagoClient.delete("ColFam".getBytes(), "Key".getBytes());

    ByteBuf bb = ch1.readOutbound();
    byte[] _bb = new byte[bb.readableBytes()];
    bb.readBytes(_bb);
    ChicagoMessage chicagoMessage = decoder.decode(_bb);

    assertEquals(Op.DELETE, chicagoMessage.getOp());
    assertEquals("ColFam", new String(chicagoMessage.getColFam()));
    assertEquals("Key", new String(chicagoMessage.getKey()));
  }

}