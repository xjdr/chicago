package com.xjeffrose.chicago;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ChicagoObjectDecoderTest {
  private ChicagoObjectEncoder encoder = new ChicagoObjectEncoder();
  private ChicagoObjectDecoder decoder = new ChicagoObjectDecoder();


  @Test
  public void decode() throws Exception {

    byte[] result = encoder.encode(Op.fromInt(0), "default".getBytes(), "foo".getBytes(), "asdfgjlkasdf".getBytes());

    ChicagoMessage message = decoder.decode(result);

    assertEquals(Op.READ, message.getOp());
    assertEquals("default", new String(message.getColFam()));
    assertEquals("foo", new String(message.getKey()));
    assertEquals("asdfgjlkasdf", new String(message.getVal()));

  }

}