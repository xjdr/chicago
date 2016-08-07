package com.xjeffrose.chicago.client;

import com.google.common.util.concurrent.SettableFuture;
import com.xjeffrose.chicago.DefaultChicagoMessage;
import com.xjeffrose.chicago.Op;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.internal.PlatformDependent;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ChicagoClientHandlerTest {
  @Test
  public void channelRead0() throws Exception {
    Map<UUID, SettableFuture<byte[]>> futureMap = new ConcurrentHashMap<>();
    Map<UUID, SettableFuture<byte[]>> reqMap = PlatformDependent.newConcurrentHashMap();
    EmbeddedChannel ch = new EmbeddedChannel(new ChicagoClientHandler(futureMap));

    for (int i = 0; i < 100; i++) {
      UUID id = UUID.randomUUID();
      SettableFuture<byte[]> f = SettableFuture.create();
      futureMap.put(id, f);
      reqMap.put(id, f);
      ch.writeInbound(new DefaultChicagoMessage(id, Op.RESPONSE, "ColFam".getBytes(), "Key".getBytes(), ("Val" + i).getBytes()));
    }

    assertEquals(100, reqMap.size());

    reqMap.keySet().stream().forEach(xs -> {
      try {
        SettableFuture<byte[]> _f = reqMap.get(xs);
        byte[] ba = _f.get();
        assertTrue(ba.length > 0);
        assertTrue(new String(ba).contains("Val"));
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ExecutionException e) {
        e.printStackTrace();
      }
    });


  }

}