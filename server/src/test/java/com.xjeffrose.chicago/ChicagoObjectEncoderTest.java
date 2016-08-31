package com.xjeffrose.chicago;

import com.xjeffrose.chicago.db.DBManager;
import com.xjeffrose.chicago.db.InMemDBImpl;
import com.xjeffrose.chicago.db.StorageProvider;
import com.xjeffrose.chicago.server.ChicagoDBHandler;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class ChicagoObjectEncoderTest {

  @Test
  public void encode() throws Exception {
    ChicagoObjectEncoder encoder = new ChicagoObjectEncoder();
    StorageProvider db = new InMemDBImpl();
    DBManager dbManager = new DBManager(db);
    dbManager.startAsync().awaitRunning();


    EmbeddedChannel ch1 = new EmbeddedChannel(new ChannelInitializer<EmbeddedChannel>() {
      @Override
      protected void initChannel(EmbeddedChannel channel) throws Exception {
        ChannelPipeline cp = channel.pipeline();
        cp.addLast(new ChicagoObjectDecoder());
        cp.addLast(new ChicagoDBHandler(dbManager));
      }
    });

    ChicagoMessage cm1 = new DefaultChicagoMessage(UUID.randomUUID(), Op.WRITE, "colfFam".getBytes(), "key1".getBytes(), "val1".getBytes());
    ChicagoMessage cm2 = new DefaultChicagoMessage(UUID.randomUUID(), Op.WRITE, "colfFam".getBytes(), "key2".getBytes(), "val2".getBytes());
    ChicagoMessage cm3 = new DefaultChicagoMessage(UUID.randomUUID(), Op.WRITE, "colfFam".getBytes(), "key3".getBytes(), "val3".getBytes());
    ChicagoMessage cm4 = new DefaultChicagoMessage(UUID.randomUUID(), Op.WRITE, "colfFam".getBytes(), "key4".getBytes(), "val4".getBytes());


    ByteBuf bb = Unpooled.buffer();

    bb.writeBytes(encoder.encode(cm1));
    bb.writeBytes(encoder.encode(cm2));
    bb.writeBytes(encoder.encode(cm3));
    bb.writeBytes(encoder.encode(cm4));

    ch1.writeInbound(bb);

    dbManager.waitForEmptyQueue().get();

    assertEquals("val1", new String(db.read("colfFam".getBytes(), "key1".getBytes())));
    assertEquals("val2", new String(db.read("colfFam".getBytes(), "key2".getBytes())));
    assertEquals("val3", new String(db.read("colfFam".getBytes(), "key3".getBytes())));
    assertEquals("val4", new String(db.read("colfFam".getBytes(), "key4".getBytes())));


  }

}
