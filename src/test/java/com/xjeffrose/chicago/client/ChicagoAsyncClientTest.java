package com.xjeffrose.chicago.client;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.xjeffrose.chicago.ChicagoMessage;
import com.xjeffrose.chicago.ChicagoObjectDecoder;
import com.xjeffrose.chicago.ChicagoObjectEncoder;
import com.xjeffrose.chicago.DefaultChicagoMessage;
import com.xjeffrose.chicago.Op;
import com.xjeffrose.chicago.server.Chicago;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
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

import static org.junit.Assert.*;

public class ChicagoAsyncClientTest {
  private final static String NODE_LIST_PATH = "/chicago/node-list";
  private final Map<UUID, SettableFuture<byte[]>> futureMap = PlatformDependent.newConcurrentHashMap();
  private EmbeddedChannel ch1 = new EmbeddedChannel(new ChannelInitializer<EmbeddedChannel>() {
    @Override
    protected void initChannel(EmbeddedChannel channel) throws Exception {
      ChannelPipeline cp = channel.pipeline();
      cp.addLast(new ChicagoClientCodec());
      cp.addLast(new ChicagoClientHandler(futureMap));
    }
  });

  private ChicagoObjectDecoder decoder = new ChicagoObjectDecoder();
  private ChicagoObjectEncoder encoder = new ChicagoObjectEncoder();
  private ChicagoAsyncClient chicagoClient;

  @Before
  public void setUp() throws Exception {
    try (ChicagoAsyncClient chicagoClient = new ChicagoAsyncClient(ch1, futureMap, 1)) {
      this.chicagoClient = chicagoClient;
      chicagoClient.start();
    }
  }

  @After
  public void tearDown() throws Exception {
    chicagoClient.close();
  }

  @Test
  public void read() throws Exception {
    chicagoClient.read("colFam".getBytes(), "key".getBytes());

    ByteBuf bb = ch1.readOutbound();
    byte[] _bb = new byte[bb.readableBytes()];
    bb.readBytes(_bb);
    ChicagoMessage chicagoMessage = decoder.decode(_bb);

    assertEquals(Op.READ, chicagoMessage.getOp());
    assertEquals("colFam", new String(chicagoMessage.getColFam()));
    assertEquals("ley", new String(chicagoMessage.getKey()));
  }


  @Test
  public void readHappyRespPath() throws Exception {
    ListenableFuture<byte[]> clientResp = chicagoClient.read("ColFam".getBytes(), "Key".getBytes());

    ByteBuf bb = ch1.readOutbound();
    byte[] _bb = new byte[bb.readableBytes()];
    bb.readBytes(_bb);
    ChicagoMessage chicagoMessage = decoder.decode(_bb);

    ch1.writeInbound(new DefaultChicagoMessage(chicagoMessage.getId(), Op.RESPONSE, "colFam".getBytes(), Boolean.toString(true).getBytes(), "val".getBytes()));
    CountDownLatch latch = new CountDownLatch(1);
    Futures.addCallback(clientResp, new FutureCallback<byte[]>() {
          @Override
          public void onSuccess(@Nullable byte[] bytes) {
            assertEquals("val", new String(bytes));
            latch.countDown();
          }

          @Override
          public void onFailure(Throwable throwable) {
            assertTrue(false);
          }
        });

        latch.await();
  }

  @Test
  public void write() throws Exception {
    ListenableFuture<Boolean> clientResp = chicagoClient.write("ColFam".getBytes(), "Key".getBytes(), "Val".getBytes());

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
    ListenableFuture<Boolean> clientResp = chicagoClient.write("ColFam".getBytes(), "Key".getBytes(), "Val".getBytes());

    ByteBuf bb = ch1.readOutbound();
    byte[] _bb = new byte[bb.readableBytes()];
    bb.readBytes(_bb);
    ChicagoMessage chicagoMessage = decoder.decode(_bb);

    ch1.writeInbound(new DefaultChicagoMessage(chicagoMessage.getId(), Op.RESPONSE, "ColFam".getBytes(), Boolean.toString(true).getBytes(), null));
    CountDownLatch latch = new CountDownLatch(1);
    Futures.addCallback(clientResp, new FutureCallback<Boolean>() {
      @Override
      public void onSuccess(@Nullable Boolean aBoolean) {
        latch.countDown();
      }

      @Override
      public void onFailure(Throwable throwable) {

      }
    });

    latch.await();
  }


  @Test
  public void tsWrite() throws Exception {

  }

  @Test
  public void batchWrite() throws Exception {

  }

  @Test
  public void stream() throws Exception {

  }

}