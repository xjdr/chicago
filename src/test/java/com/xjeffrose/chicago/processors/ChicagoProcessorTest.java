package com.xjeffrose.chicago.processors;

import com.google.common.util.concurrent.ListenableFuture;
import com.typesafe.config.ConfigFactory;
import com.xjeffrose.chicago.ChiConfig;
import com.xjeffrose.chicago.DBManager;
import com.xjeffrose.chicago.fixtures.TestCTX;
import com.xjeffrose.xio.core.ConnectionContext;
import com.xjeffrose.xio.server.RequestContext;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ChicagoProcessorTest {
  ChicagoRequestDecoder chicagoRequestDecoder = new ChicagoRequestDecoder();
  DBManager dbManager = new DBManager(new ChiConfig(ConfigFactory.parseFile(new File("test.conf"))));
  ChicagoProcessor processor = new ChicagoProcessor(dbManager);

  @Test
  public void process() throws Exception {

    byte[] op = {1};
    byte[] key = "new_key".getBytes();
    byte[] keySize = {(byte) key.length};
    byte[] val = "new_value".getBytes();
    byte[] valSize = {(byte) val.length};

    byte[] msgArray = new byte[op.length + keySize.length + key.length + valSize.length + val.length];

    System.arraycopy(op, 0, msgArray, 0, op.length);
    System.arraycopy(keySize, 0, msgArray, op.length, keySize.length);
    System.arraycopy(key, 0, msgArray, op.length + keySize.length, key.length);
    System.arraycopy(valSize, 0, msgArray, op.length + keySize.length + key.length, valSize.length);
    System.arraycopy(val, 0, msgArray, op.length + keySize.length + key.length + valSize.length, val.length);

    ByteBuf msg = Unpooled.wrappedBuffer(msgArray);
    List<Object> list = new ArrayList<>();
    chicagoRequestDecoder.decode(null, msg, list);

    ListenableFuture<Boolean> processFuture = processor.process(new TestCTX(), list, new RequestContext() {
      @Override
      public ConnectionContext getConnectionContext() {
        return null;
      }

      @Override
      public void setContextData(UUID uuid, Object o) {

      }

      @Override
      public Object getContextData(UUID uuid) {
        return null;
      }

      @Override
      public void clearContextData(UUID uuid) {

      }

      @Override
      public Iterator<Map.Entry<UUID, Object>> contextDataIterator() {
        return null;
      }

      @Override
      public UUID getConnectionId() {
        return null;
      }
    });

    assertEquals(true, processFuture.get());
    assertEquals(new String(val), new String(dbManager.read(key)));
    assertTrue(dbManager.delete(key));


  }

}