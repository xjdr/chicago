package com.xjeffrose.chicago.client;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.xjeffrose.chicago.ChicagoCodec;
import com.xjeffrose.chicago.ChicagoMessage;
import com.xjeffrose.chicago.ChicagoObjectDecoder;
import com.xjeffrose.chicago.ChicagoObjectEncoder;
import com.xjeffrose.chicago.DefaultChicagoMessage;
import com.xjeffrose.chicago.Op;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.internal.PlatformDependent;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import javax.annotation.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ChicagoClientTest {
  private final static String NODE_LIST_PATH = "/chicago/node-list";
  private final Map<UUID, SettableFuture<byte[]>> futureMap = PlatformDependent.newConcurrentHashMap();
  private EmbeddedChannel ch1 = new EmbeddedChannel(new ChannelInitializer<EmbeddedChannel>() {
    @Override
    protected void initChannel(EmbeddedChannel channel) throws Exception {
      ChannelPipeline cp = channel.pipeline();
      cp.addLast(new ChicagoCodec());
      cp.addLast(new ChicagoClientHandler(futureMap));
    }
  });

  private ChicagoObjectDecoder decoder = new ChicagoObjectDecoder();
  private ChicagoObjectEncoder encoder = new ChicagoObjectEncoder();
  private ChicagoClient chicagoClient;

  @Before
  public void setUp() throws Exception {
    List<EmbeddedChannel> hostList = new ArrayList<EmbeddedChannel>();
    hostList.add(ch1);

    try (ChicagoClient chicagoClient = new ChicagoClient(hostList, futureMap, 1)) {
      this.chicagoClient = chicagoClient;
    }
  }

  @After
  public void tearDown() throws Exception {
    chicagoClient.close();
  }

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
  public void writeHappyReqPath() throws Exception {
    ListenableFuture<List<byte[]>> clientResp = chicagoClient.write("ColFam".getBytes(), "Key".getBytes(), "Val".getBytes());

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
  public void writeHappyRespPath() throws Exception {
    ListenableFuture<List<byte[]>> clientResp = chicagoClient.write("ColFam".getBytes(), "Key".getBytes(), "Val".getBytes());

    ByteBuf bb = ch1.readOutbound();
    byte[] _bb = new byte[bb.readableBytes()];
    bb.readBytes(_bb);
    ChicagoMessage chicagoMessage = decoder.decode(_bb);

    ch1.writeInbound(new DefaultChicagoMessage(chicagoMessage.getId(), Op.RESPONSE, "ColFam".getBytes(), Boolean.toString(true).getBytes(), null));
    CountDownLatch latch = new CountDownLatch(1);
    Futures.addCallback(clientResp, new FutureCallback<List<byte[]>>() {
      @Override
      public void onSuccess(@Nullable List<byte[]> bytes) {
        assertNotNull(bytes);
        latch.countDown();
      }

      @Override
      public void onFailure(Throwable throwable) {
        assertTrue(false);
        latch.countDown();
      }
    });

    latch.await();
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

//  @Test
//  public void tsbatchWrite() throws Exception {
//    for (int i = 0; i < 10000; i++) {
//      String key = "Key";
//      String val = "Val" + i;
//      chicagoClient.tsbatchWrite(key.getBytes(), val.getBytes());
//    }
//
//    ByteBuf bb = ch1.readOutbound();
//    byte[] _bb = new byte[bb.readableBytes()];
//    bb.readBytes(_bb);
//    ChicagoMessage chicagoMessage = decoder.decode(_bb);
//
//    assertEquals(Op.TS_WRITE, chicagoMessage.getOp());
//    assertEquals("Key", new String(chicagoMessage.getColFam()));
//    assertTrue(new String(chicagoMessage.getVal()).contains("Val0"));
//  }

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
