package com.xjeffrose.chicago;

import com.google.common.util.concurrent.ListenableFuture;
import com.typesafe.config.ConfigFactory;
import com.xjeffrose.chicago.ChiConfig;
import com.xjeffrose.chicago.ChicagoObjectDecoder;
import com.xjeffrose.chicago.ChicagoProcessor;
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
  DBManager dbManager = new DBManager(new ChiConfig(ConfigFactory.parseFile(new File("test.conf"))));
  ChicagoProcessor processor = new ChicagoProcessor(dbManager);

  @Test
  public void process() throws Exception {
    byte[] key = "new_key".getBytes();
    byte[] val = "new_value".getBytes();

    ListenableFuture<Boolean> processFuture = processor.process(new TestCTX(), new DefaultChicagoMessage(Op.fromInt(1), key, val), new RequestContext() {
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

    processFuture.get();
    assertEquals(new String(val), new String(dbManager.read(key)));
    assertTrue(dbManager.delete(key));


  }

}