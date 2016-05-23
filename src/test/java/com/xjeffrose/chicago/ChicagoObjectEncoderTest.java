package com.xjeffrose.chicago;

import java.util.UUID;
import org.junit.Test;

import static org.junit.Assert.*;

public class ChicagoObjectEncoderTest {
  ChicagoObjectEncoder encoder = new ChicagoObjectEncoder();
  ChicagoObjectDecoder decoder = new ChicagoObjectDecoder();

  @Test
  public void encode1() throws Exception {
    byte[] result = encoder.encode(UUID.randomUUID(), Op.fromInt(0), "colFamxxxx".getBytes(), "foo".getBytes(), "asdfgjlkasdf".getBytes());

    ChicagoMessage message = decoder.decode(result);

    assertEquals(Op.READ, message.getOp());
    assertEquals("colFamxxxx", new String(message.getColFam()));
    assertEquals("foo", new String(message.getKey()));
    assertEquals("asdfgjlkasdf", new String(message.getVal()));
  }


  @Test
  public void encode() throws Exception {
//    encoder.encode(null, );
  }

}