package com.xjeffrose.chicago;

import com.xjeffrose.chicago.db.DBManager;
import com.xjeffrose.chicago.db.InMemDBImpl;
import com.xjeffrose.chicago.server.ChicagoDBHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import java.util.UUID;

// TODO(CK): change this into three tests
public class ChicagoDBHandlerTest extends org.junit.Assert {
  @Test
  public void channelRead0() throws Exception {

    InMemDBImpl inMemDB = new InMemDBImpl();
    DBManager manager = new DBManager(inMemDB);
    manager.startAsync().awaitRunning();
    EmbeddedChannel ch = new EmbeddedChannel(new ChicagoDBHandler(manager, new ChicagoPaxosClient("")));
    UUID id = UUID.randomUUID();

    for (int i = 0; i < 100; i++) {
      ch.writeInbound(new DefaultChicagoMessage(id, Op.WRITE, "ColFam".getBytes(), ("Key" + i).getBytes(), ("Val" + i).getBytes()));
    }

    manager.waitForEmptyQueue().get();
    ch.runPendingTasks();

    for (int i = 0; i < 100; i++) {
      assertEquals(("Val" + i), new String(inMemDB.read("ColFam".getBytes(), ("Key" + i).getBytes())));

      manager.waitForEmptyQueue().get();
      ch.runPendingTasks();
      ChicagoMessage msg = ch.readOutbound();
      assertNotNull(msg);
      assertEquals(id, msg.getId());
      assertEquals(Op.RESPONSE, msg.getOp());
      assertTrue((Boolean.valueOf(new String(msg.getKey()))));

    }

    for (int i = 0; i < 100; i++) {
      ch.writeInbound(new DefaultChicagoMessage(id, Op.READ, "ColFam".getBytes(), ("Key" + i).getBytes(), null));

      manager.waitForEmptyQueue().get();
      ch.runPendingTasks();

      ChicagoMessage msg = ch.readOutbound();
      assertNotNull(msg);
      assertEquals(id, msg.getId());
      assertEquals(Op.RESPONSE, msg.getOp());
      assertTrue((Boolean.valueOf(new String(msg.getKey()))));
      assertEquals(("Val" + i), new String(msg.getVal()));
      assertTrue(msg.getSuccess());

    }

    manager.stopAsync().awaitTerminated();
  }
}
