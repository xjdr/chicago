package com.xjeffrose.chicago.server;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.xjeffrose.chicago.ChicagoMessage;
import com.xjeffrose.chicago.DefaultChicagoMessage;
import com.xjeffrose.chicago.Op;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.internal.PlatformDependent;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

import static org.junit.Assert.*;

public class ChicagoGlobalOffsetManagerHandlerTest {
  private Map<String, AtomicLong> offset;
  private Map<String, Integer> q;
  private Map<String, Map<String, Long>> sessionCoordinator;
  private Map<String, AtomicInteger> qCount;

  @Test
  public void getOffsetHappyPath() throws Exception {
    this.offset = PlatformDependent.newConcurrentHashMap();
    this.q = PlatformDependent.newConcurrentHashMap();
    this.sessionCoordinator = PlatformDependent.newConcurrentHashMap();
    this.qCount = PlatformDependent.newConcurrentHashMap();

    ChicagoGlobalOffsetManagerHandler offsetManagerHandler = new ChicagoGlobalOffsetManagerHandler(offset, q, sessionCoordinator, qCount);

    EmbeddedChannel ch1 = new EmbeddedChannel(offsetManagerHandler);
    EmbeddedChannel ch2 = new EmbeddedChannel(offsetManagerHandler);
    EmbeddedChannel ch3 = new EmbeddedChannel(offsetManagerHandler);
    EmbeddedChannel ch4 = new EmbeddedChannel(offsetManagerHandler);

    ChicagoMessage cm1 = new DefaultChicagoMessage(UUID.randomUUID(), Op.GET_OFFSET, "ColFam".getBytes(), "id1".getBytes(), Ints.toByteArray(3));
    ChicagoMessage cm2 = new DefaultChicagoMessage(UUID.randomUUID(), Op.GET_OFFSET, "ColFam".getBytes(), "id2".getBytes(), Ints.toByteArray(3));
    ChicagoMessage cm3 = new DefaultChicagoMessage(UUID.randomUUID(), Op.GET_OFFSET, "ColFam".getBytes(), "id3".getBytes(), Ints.toByteArray(3));
    ChicagoMessage cm4 = new DefaultChicagoMessage(UUID.randomUUID(), Op.GET_OFFSET, "ColFam".getBytes(), "id4".getBytes(), Ints.toByteArray(3));

    ch1.writeInbound(cm1);
    ch2.writeInbound(cm2);
    ch3.writeInbound(cm3);
    ch4.writeInbound(cm4);

    ChicagoMessage resp1 = ch1.readOutbound();
    ChicagoMessage resp2 = ch2.readOutbound();
    ChicagoMessage resp3 = ch3.readOutbound();
    ChicagoMessage resp4 = ch4.readOutbound();

    assertEquals(0, Longs.fromByteArray(resp1.getVal()));
    assertEquals(0, Longs.fromByteArray(resp2.getVal()));
    assertEquals(0, Longs.fromByteArray(resp3.getVal()));
    assertEquals(1, Longs.fromByteArray(resp4.getVal()));
  }

}