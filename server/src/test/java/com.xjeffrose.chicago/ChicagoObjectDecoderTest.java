package com.xjeffrose.chicago;

import com.xjeffrose.chicago.db.DBManager;
import com.xjeffrose.chicago.db.InMemDBImpl;
import com.xjeffrose.chicago.db.StorageProvider;
import com.xjeffrose.chicago.server.ChicagoDBHandler;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.UUID;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class ChicagoObjectDecoderTest {

  ChicagoObjectEncoder encoder;
  ChicagoObjectDecoder decoder;

  @Before
  public void beforeMethod() {
    encoder = new ChicagoObjectEncoder();
    decoder = new ChicagoObjectDecoder();
  }

  @Test
  public void decode() throws Exception {
    StorageProvider db = new InMemDBImpl();
    DBManager dbManager = new DBManager(db);
    dbManager.startAsync().awaitRunning();

    EmbeddedChannel ch1 = new EmbeddedChannel(decoder, new ChicagoDBHandler(dbManager, new ChicagoPaxosClient("")));

    ChicagoMessage cm1 =
      new DefaultChicagoMessage(UUID.randomUUID(), Op.WRITE, "colfFam".getBytes(),
        "key1".getBytes(), "val1".getBytes());
    ChicagoMessage cm2 =
      new DefaultChicagoMessage(UUID.randomUUID(), Op.WRITE, "colfFam".getBytes(),
        "key2".getBytes(), "val2".getBytes());
    ChicagoMessage cm3 =
      new DefaultChicagoMessage(UUID.randomUUID(), Op.WRITE, "colfFam".getBytes(),
        "key3".getBytes(), "val3".getBytes());
    ChicagoMessage cm4 =
      new DefaultChicagoMessage(UUID.randomUUID(), Op.WRITE, "colfFam".getBytes(),
        "key4".getBytes(), "val4".getBytes());

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

  @Test
  public void testDecode_oneBigMessage() {
    EmbeddedChannel channel = new EmbeddedChannel(decoder);

    ChicagoMessage cm1 =
      new DefaultChicagoMessage(UUID.randomUUID(), Op.WRITE, "colfFam".getBytes(),
        "key1".getBytes(), "val1".getBytes());
    ChicagoMessage cm2 =
      new DefaultChicagoMessage(UUID.randomUUID(), Op.WRITE, "colfFam".getBytes(),
        "key2".getBytes(), "val2".getBytes());
    ChicagoMessage cm3 =
      new DefaultChicagoMessage(UUID.randomUUID(), Op.WRITE, "colfFam".getBytes(),
        "key3".getBytes(), "val3".getBytes());
    ChicagoMessage cm4 =
      new DefaultChicagoMessage(UUID.randomUUID(), Op.WRITE, "colfFam".getBytes(),
        "key4".getBytes(), "val4".getBytes());

    ByteBuf message1 = Unpooled.buffer();
    message1.writeBytes(encoder.encode(cm1));
    message1.writeBytes(encoder.encode(cm2));
    message1.writeBytes(encoder.encode(cm3));
    message1.writeBytes(encoder.encode(cm4));

    channel.writeInbound(message1);

    assertEquals(getFirstMessageString(channel), 4, channel.inboundMessages().size());
    assertEquals(cm1, channel.inboundMessages().poll());
    assertEquals(cm2, channel.inboundMessages().poll());
    assertEquals(cm3, channel.inboundMessages().poll());
    assertEquals(cm4, channel.inboundMessages().poll());
  }

  @Test
  public void testDecode_partialMessages() {
    EmbeddedChannel channel = new EmbeddedChannel(decoder);

    ChicagoMessage cm1 =
      new DefaultChicagoMessage(UUID.randomUUID(), Op.WRITE, "colfFam".getBytes(),
        "key1".getBytes(), "val1".getBytes());
    ChicagoMessage cm2 =
      new DefaultChicagoMessage(UUID.randomUUID(), Op.WRITE, "colfFam".getBytes(),
        "key2".getBytes(), "val2".getBytes());
    ChicagoMessage cm3 =
      new DefaultChicagoMessage(UUID.randomUUID(), Op.WRITE, "colfFam".getBytes(),
        "key3".getBytes(), "val3".getBytes());
    ChicagoMessage cm4 =
      new DefaultChicagoMessage(UUID.randomUUID(), Op.WRITE, "colfFam".getBytes(),
        "key4".getBytes(), "val4".getBytes());

    ByteBuf message1 = Unpooled.buffer();
    message1.writeBytes(encoder.encode(cm1));
    message1.writeBytes(encoder.encode(cm2));
    message1.writeBytes(encoder.encode(cm3));
    message1.writeBytes(encoder.encode(cm4));

    int messageLength = message1.writerIndex();
    for (int offset = 0; offset < messageLength; offset++) {
      ByteBuf tmp = Unpooled.wrappedBuffer(message1.array(), offset, 1);
      channel.writeInbound(tmp);
    }


    assertEquals(getFirstMessageString(channel), 4, channel.inboundMessages().size());
    assertEquals(cm1, channel.inboundMessages().poll());
    assertEquals(cm2, channel.inboundMessages().poll());
    assertEquals(cm3, channel.inboundMessages().poll());
    assertEquals(cm4, channel.inboundMessages().poll());
  }

  @Test
  public void testDecode_multipleMessages() {
    EmbeddedChannel channel = new EmbeddedChannel(decoder);

    ChicagoMessage cm1 =
      new DefaultChicagoMessage(UUID.randomUUID(), Op.WRITE, "colfFam".getBytes(),
        "key1".getBytes(), "val1".getBytes());
    ChicagoMessage cm2 =
      new DefaultChicagoMessage(UUID.randomUUID(), Op.WRITE, "colfFam".getBytes(),
        "key2".getBytes(), "val2".getBytes());
    ChicagoMessage cm3 =
      new DefaultChicagoMessage(UUID.randomUUID(), Op.WRITE, "colfFam".getBytes(),
        "key3".getBytes(), "val3".getBytes());
    ChicagoMessage cm4 =
      new DefaultChicagoMessage(UUID.randomUUID(), Op.WRITE, "colfFam".getBytes(),
        "key4".getBytes(), "val4".getBytes());

    ByteBuf message1 = Unpooled.buffer();
    message1.writeBytes(encoder.encode(cm1));
    message1.writeBytes(encoder.encode(cm2));

    ByteBuf message2 = Unpooled.buffer();
    message2.writeBytes(encoder.encode(cm3));
    message2.writeBytes(encoder.encode(cm4));

    channel.writeInbound(message1);
    channel.writeInbound(message2);

    assertEquals(getFirstMessageString(channel), 4, channel.inboundMessages().size());
    assertEquals(cm1, channel.inboundMessages().poll());
    assertEquals(cm2, channel.inboundMessages().poll());
    assertEquals(cm3, channel.inboundMessages().poll());
    assertEquals(cm4, channel.inboundMessages().poll());
  }

  @Test
  public void testDecode_multipleMessagesSameID() {
    EmbeddedChannel channel = new EmbeddedChannel(decoder);

    UUID sharedId = UUID.randomUUID();
    ChicagoMessage cm1 =
      new DefaultChicagoMessage(sharedId, Op.WRITE, "colfFam".getBytes(), "key1".getBytes(),
        "val1".getBytes());
    ChicagoMessage cm2 =
      new DefaultChicagoMessage(sharedId, Op.WRITE, "colfFam".getBytes(), "key2".getBytes(),
        "val2".getBytes());
    ChicagoMessage cm3 =
      new DefaultChicagoMessage(sharedId, Op.WRITE, "colfFam".getBytes(), "key3".getBytes(),
        "val3".getBytes());
    ChicagoMessage cm4 =
      new DefaultChicagoMessage(sharedId, Op.WRITE, "colfFam".getBytes(), "key4".getBytes(),
        "val4".getBytes());

    ByteBuf message1 = Unpooled.buffer();
    message1.writeBytes(encoder.encode(cm1));
    message1.writeBytes(encoder.encode(cm2));

    ByteBuf message2 = Unpooled.buffer();
    message2.writeBytes(encoder.encode(cm3));
    message2.writeBytes(encoder.encode(cm4));

    channel.writeInbound(message1);
    channel.writeInbound(message2);

    assertEquals(getFirstMessageString(channel), 4, channel.inboundMessages().size());
    assertEquals(cm1, channel.inboundMessages().poll());
    assertEquals(cm2, channel.inboundMessages().poll());
    assertEquals(cm3, channel.inboundMessages().poll());
    assertEquals(cm4, channel.inboundMessages().poll());
  }

  private String getFirstMessageString(EmbeddedChannel channel) {
    return channel.inboundMessages().peek().toString();
  }
}
