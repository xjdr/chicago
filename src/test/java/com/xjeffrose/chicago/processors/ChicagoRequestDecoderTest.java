package com.xjeffrose.chicago.processors;

import com.xjeffrose.chicago.processors.ChicagoRequestDecoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.*;

public class ChicagoRequestDecoderTest {
  ChicagoRequestDecoder chicagoRequestDecoder = new ChicagoRequestDecoder();


  @Test
  public void decode() throws Exception {

    byte[] op = {0};
    byte[] key = "key".getBytes();
    byte[] keySize = {(byte) key.length};
    byte[] val = "value".getBytes();
    byte[] valSize ={(byte) val.length};

    byte[] msgArray = new byte[op.length + keySize.length + key.length + valSize.length + val.length];

    System.arraycopy(op, 0, msgArray, 0, op.length);
    System.arraycopy(keySize, 0, msgArray, op.length, keySize.length);
    System.arraycopy(key, 0, msgArray, op.length + keySize.length, key.length);
    System.arraycopy(valSize, 0, msgArray, op.length + keySize.length + key.length , valSize.length);
    System.arraycopy(val, 0, msgArray, op.length + keySize.length + key.length + valSize.length, val.length);

    ByteBuf msg = Unpooled.wrappedBuffer(msgArray);
    List<Object> list = new ArrayList<>();
    chicagoRequestDecoder.decode(null, msg, list);

    int _op = (int) list.get(0);
    byte[] _key = (byte[]) list.get(1);
    byte[] _val = (byte[]) list.get(2);

    assertEquals(0, _op);
    assertEquals("key", new String(_key));
    assertEquals("value", new String(_val));

  }

}