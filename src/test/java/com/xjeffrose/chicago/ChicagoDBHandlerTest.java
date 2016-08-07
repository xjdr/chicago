package com.xjeffrose.chicago;

import com.xjeffrose.chicago.server.ChicagoDBHandler;
import com.xjeffrose.chicago.server.DBLog;
import com.xjeffrose.chicago.server.InMemDBImpl;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.UUID;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ChicagoDBHandlerTest {
  @Test
  public void channelRead0() throws Exception {

    InMemDBImpl inMemDB = new InMemDBImpl();
    EmbeddedChannel ch = new EmbeddedChannel(new ChicagoDBHandler(inMemDB, new DBLog()));
    ChicagoObjectDecoder decoder = new ChicagoObjectDecoder();
    UUID id = UUID.randomUUID();

    for (int i = 0; i < 100; i++) {
      ch.writeInbound(new DefaultChicagoMessage(id, Op.WRITE, "ColFam".getBytes(), ("Key" + i).getBytes(), ("Val" + i).getBytes()));
    }

    for (int i = 0; i < 100; i++) {
      assertEquals(id, decoder.decode(inMemDB.read("ColFam".getBytes(), ("Key" + i).getBytes())).getId());
      assertEquals(Op.WRITE, decoder.decode(inMemDB.read("ColFam".getBytes(), ("Key" + i).getBytes())).getOp());
      assertEquals(("Key" + i), new String(decoder.decode(inMemDB.read("ColFam".getBytes(), ("Key" + i).getBytes())).getKey()));
      assertEquals(("Val" + i), new String(decoder.decode(inMemDB.read("ColFam".getBytes(), ("Key" + i).getBytes())).getVal()));

      ChicagoMessage msg = ch.readOutbound();
      assertEquals(id, msg.getId());
      assertEquals(Op.RESPONSE, msg.getOp());
      assertTrue((Boolean.valueOf(new String(msg.getKey()))));

    }

    for (int i = 0; i < 100; i++) {
      ch.writeInbound(new DefaultChicagoMessage(id, Op.READ, "ColFam".getBytes(), ("Key" + i).getBytes(), null));

//      assertEquals(id, decoder.decode(inMemDB.read("ColFam".getBytes(), ("Key" + i).getBytes())).getId());
//      assertEquals(Op.READ, decoder.decode(inMemDB.read("ColFam".getBytes(), ("Key" + i).getBytes())).getOp());
//      assertEquals(("Key" + i), new String(decoder.decode(inMemDB.read("ColFam".getBytes(), ("Key" + i).getBytes())).getKey()));

      ChicagoMessage msg = ch.readOutbound();
      assertEquals(id, msg.getId());
      assertEquals(Op.RESPONSE, msg.getOp());
      assertTrue((Boolean.valueOf(new String(msg.getKey()))));
      assertEquals(("Val" + i), new String(decoder.decode(msg.getVal()).getVal()));
      assertTrue(msg.getSuccess());

    }
  }
}