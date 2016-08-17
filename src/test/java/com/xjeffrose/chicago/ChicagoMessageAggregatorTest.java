package com.xjeffrose.chicago;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 *
 */
public class ChicagoMessageAggregatorTest {

  private ChicagoObjectEncoder encoder;
  private ChicagoObjectDecoder decoder;
  private ChicagoMessageAggregator aggregator;

  @Before
  public void beforeMethod() {
    encoder = new ChicagoObjectEncoder();
    decoder = new ChicagoObjectDecoder();
    aggregator = new ChicagoMessageAggregator();
  }

  @Test
  public void testDecode_streamMultipleMessages() {
    EmbeddedChannel channel = new EmbeddedChannel(decoder, aggregator);

    ChicagoMessage cm1 =
      new DefaultChicagoMessage(UUID.randomUUID(), Op.STREAM_RESPONSE, "colfFam".getBytes(),
        "key1".getBytes(), "val1@@@".getBytes());
    ChicagoMessage cm2 =
      new DefaultChicagoMessage(UUID.randomUUID(), Op.STREAM_RESPONSE, "colfFam".getBytes(),
        "key2".getBytes(), "val2@@@".getBytes());
    ChicagoMessage cm3 =
      new DefaultChicagoMessage(UUID.randomUUID(), Op.STREAM_RESPONSE, "colfFam".getBytes(),
        "key3".getBytes(), "val3@@@".getBytes());
    ChicagoMessage cm4 =
      new DefaultChicagoMessage(UUID.randomUUID(), Op.STREAM_RESPONSE, "colfFam".getBytes(),
        "key4".getBytes(), "val4@@@".getBytes());

    ByteBuf message1 = Unpooled.buffer();
    message1.writeBytes(encoder.encode(cm1));
    message1.writeBytes(encoder.encode(cm2));

    ByteBuf message2 = Unpooled.buffer();
    message2.writeBytes(encoder.encode(cm3));
    message2.writeBytes(encoder.encode(cm4));

    channel.writeInbound(message1);
    channel.writeInbound(message2);

    assertEquals(getFirstMessageString(channel), 4, channel.inboundMessages().size());
    assertStreamResponse(channel, cm1);
    assertStreamResponse(channel, cm2);
    assertStreamResponse(channel, cm3);
    assertStreamResponse(channel, cm4);
  }

  @Test
  public void testDecode_streamMultipleMessagesSameId() {
    EmbeddedChannel channel = new EmbeddedChannel(decoder, aggregator);

    UUID sharedId = UUID.randomUUID();
    ChicagoMessage cm1 =
      new DefaultChicagoMessage(sharedId, Op.STREAM_RESPONSE, "colfFam".getBytes(),
        "key1".getBytes(), "val1".getBytes());
    ChicagoMessage cm2 =
      new DefaultChicagoMessage(sharedId, Op.STREAM_RESPONSE, "colfFam".getBytes(),
        "key2".getBytes(), "val2".getBytes());
    ChicagoMessage cm3 =
      new DefaultChicagoMessage(sharedId, Op.STREAM_RESPONSE, "colfFam".getBytes(),
        "key3".getBytes(), "val3".getBytes());
    ChicagoMessage cm4 =
      new DefaultChicagoMessage(sharedId, Op.STREAM_RESPONSE, "colfFam".getBytes(),
        "key4".getBytes(), "val4@@@".getBytes());

    ByteBuf message1 = Unpooled.buffer();
    message1.writeBytes(encoder.encode(cm1));
    message1.writeBytes(encoder.encode(cm2));

    ByteBuf message2 = Unpooled.buffer();
    message2.writeBytes(encoder.encode(cm3));
    message2.writeBytes(encoder.encode(cm4));

    channel.writeInbound(message1);
    channel.writeInbound(message2);

    // I think a stream termination message needs to exist for this test to work.
    assertEquals(getFirstMessageString(channel), 1, channel.inboundMessages().size());
    assertStreamResponse(channel, cm1, cm2, cm3, cm4);
  }

  @Test
  public void testDecode_streamOneBigMessage() {
    EmbeddedChannel channel = new EmbeddedChannel(decoder, aggregator);

    ChicagoMessage cm1 =
      new DefaultChicagoMessage(UUID.randomUUID(), Op.STREAM_RESPONSE, "colfFam".getBytes(),
        "key1".getBytes(), "val1@@@".getBytes());
    ChicagoMessage cm2 =
      new DefaultChicagoMessage(UUID.randomUUID(), Op.STREAM_RESPONSE, "colfFam".getBytes(),
        "key2".getBytes(), "val2@@@".getBytes());
    ChicagoMessage cm3 =
      new DefaultChicagoMessage(UUID.randomUUID(), Op.STREAM_RESPONSE, "colfFam".getBytes(),
        "key3".getBytes(), "val3@@@".getBytes());
    ChicagoMessage cm4 =
      new DefaultChicagoMessage(UUID.randomUUID(), Op.STREAM_RESPONSE, "colfFam".getBytes(),
        "key4".getBytes(), "val4@@@".getBytes());

    ByteBuf message1 = Unpooled.buffer();
    message1.writeBytes(encoder.encode(cm1));
    message1.writeBytes(encoder.encode(cm2));
    message1.writeBytes(encoder.encode(cm3));
    message1.writeBytes(encoder.encode(cm4));

    channel.writeInbound(message1);

    assertEquals(getFirstMessageString(channel), 4, channel.inboundMessages().size());
    assertStreamResponse(channel, cm1);
    assertStreamResponse(channel, cm2);
    assertStreamResponse(channel, cm3);
    assertStreamResponse(channel, cm4);
  }

  @Test
  public void testDecode_streamOneBigMessageSameId() {
    EmbeddedChannel channel = new EmbeddedChannel(decoder, aggregator);

    UUID sharedId = UUID.randomUUID();
    ChicagoMessage cm1 =
      new DefaultChicagoMessage(sharedId, Op.STREAM_RESPONSE, "colfFam".getBytes(),
        "key1".getBytes(), "val1".getBytes());
    ChicagoMessage cm2 =
      new DefaultChicagoMessage(sharedId, Op.STREAM_RESPONSE, "colfFam".getBytes(),
        "key2".getBytes(), "val2".getBytes());
    ChicagoMessage cm3 =
      new DefaultChicagoMessage(sharedId, Op.STREAM_RESPONSE, "colfFam".getBytes(),
        "key3".getBytes(), "val3".getBytes());
    ChicagoMessage cm4 =
      new DefaultChicagoMessage(sharedId, Op.STREAM_RESPONSE, "colfFam".getBytes(),
        "key4".getBytes(), "val4@@@".getBytes());

    ByteBuf message1 = Unpooled.buffer();
    message1.writeBytes(encoder.encode(cm1));
    message1.writeBytes(encoder.encode(cm2));
    message1.writeBytes(encoder.encode(cm3));
    message1.writeBytes(encoder.encode(cm4));

    channel.writeInbound(message1);

    assertEquals(getFirstMessageString(channel), 1, channel.inboundMessages().size());
    assertStreamResponse(channel, cm1, cm2, cm3, cm4);
  }

  private void assertStreamResponse(EmbeddedChannel channel, ChicagoMessage firstMessage,
    ChicagoMessage... inputMessages) {
    ChicagoMessage chicagoMessage = (ChicagoMessage) channel.inboundMessages().poll();
    assertEquals(firstMessage.getId(), chicagoMessage.getId());
    assertArrayEquals(firstMessage.getColFam(), chicagoMessage.getColFam());
    assertEquals(firstMessage.getOp(), chicagoMessage.getOp());
    assertArrayEquals("true".getBytes(), chicagoMessage.getKey());
    ByteBuf expectedValue = Unpooled.buffer();
    expectedValue.writeBytes(firstMessage.getVal());
    for (ChicagoMessage cm : inputMessages) {
      expectedValue.writeByte(',').writeBytes(cm.getVal());
    }
    byte[] expectedValueArray = new byte[expectedValue.writerIndex()];
    expectedValue.readBytes(expectedValueArray);
    assertArrayEquals(new String(expectedValueArray) + " vs " + new String(chicagoMessage.getVal()),
      expectedValueArray, chicagoMessage.getVal());
  }

  private String getFirstMessageString(EmbeddedChannel channel) {
    return channel.inboundMessages().peek().toString();
  }
}
