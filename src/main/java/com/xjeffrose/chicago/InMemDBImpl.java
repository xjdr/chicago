package com.xjeffrose.chicago;

import com.google.common.primitives.Longs;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.internal.PlatformDependent;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InMemDBImpl implements DBInterface, AutoCloseable {

  private final Map<ByteBuf, Map<ByteBuf, byte[]>> db = PlatformDependent.newConcurrentHashMap();
  private final AtomicLong offset = new AtomicLong();
  private final ChiConfig config;

  public InMemDBImpl() {
    this.config = null;
  }

  public InMemDBImpl(ChiConfig config) {
    this.config = config;
  }

  @Override
  public boolean write(byte[] colFam, byte[] key, byte[] val) {
    // We are wrapping all this up as ByteBufs because the Hashmap cannot properly
    // Lookup the byte[] by hash value (same as hash code).
    // This isn't the most efficient implementation we can come up with
    // (we can do much better), but this is functional for now.
    if (db.containsKey(Unpooled.buffer().writeBytes(colFam))) {
      db.get(Unpooled.buffer().writeBytes(colFam)).put(Unpooled.buffer().writeBytes(key), val);
    } else {
      db.put(Unpooled.buffer().writeBytes(colFam), PlatformDependent.newConcurrentHashMap());
      db.get(Unpooled.buffer().writeBytes(colFam)).put(Unpooled.buffer().writeBytes(key), val);
    }

    return true;
  }

  @Override
  public byte[] read(byte[] colFam, byte[] key) {
    if (db.containsKey(Unpooled.buffer().writeBytes(colFam))) {
      if (db.get(Unpooled.buffer().writeBytes(colFam)).containsKey(Unpooled.buffer().writeBytes(key))) {
        return db.get(Unpooled.buffer().writeBytes(colFam)).get(Unpooled.buffer().writeBytes(key));
      } else {
        log.error("No such key " + new String(key) + " in colFam " + new String(colFam));
      }
    } else {
      log.error("No such colFam " + new String(colFam));
    }
    return null;
  }

  @Override
  public boolean delete(byte[] colFam, byte[] key) {
    if (db.containsKey(Unpooled.buffer().writeBytes(colFam))) {
      if (db.get(Unpooled.buffer().writeBytes(colFam)).containsKey(Unpooled.buffer().writeBytes(key))) {
         db.get(Unpooled.buffer().writeBytes(colFam)).remove(Unpooled.buffer().writeBytes(key));
        return true;
      } else {
        log.error("No such key " + new String(key) + " in colFam " + new String(colFam));
      }
    } else {
      log.error("No such colFam " + new String(colFam));
    }
    return false;
  }

  @Override
  public byte[] tsWrite(byte[] colFam, byte[] val) {
    final long _offset;
    if (db.containsKey(Unpooled.buffer().writeBytes(colFam))) {
      _offset = offset.getAndIncrement();
      db.get(Unpooled.buffer().writeBytes(colFam)).put(Unpooled.buffer().writeBytes(Longs.toByteArray(_offset)), val);
    } else {
      db.put(Unpooled.buffer().writeBytes(colFam), PlatformDependent.newConcurrentHashMap());
      _offset = offset.getAndIncrement();
      db.get(Unpooled.buffer().writeBytes(colFam)).put(Unpooled.buffer().writeBytes(Longs.toByteArray(_offset)), val);
    }

    return Longs.toByteArray(_offset);
  }

  @Override
  public byte[] batchWrite(byte[] colFam, byte[] val) {
    final long[] _offset = new long[1];
    String[] values = new String(val).split(ChiUtil.delimiter);
    Arrays.stream(values).forEach(xs -> {
    if (db.containsKey(Unpooled.buffer().writeBytes(colFam))) {
      _offset[0] = offset.getAndIncrement();
      db.get(Unpooled.buffer().writeBytes(colFam)).put(Unpooled.buffer().writeBytes(Longs.toByteArray(_offset[0])), val);
    } else {
      db.put(Unpooled.buffer().writeBytes(colFam), PlatformDependent.newConcurrentHashMap());
      _offset[0] = offset.getAndIncrement();
      db.get(Unpooled.buffer().writeBytes(colFam)).put(Unpooled.buffer().writeBytes(Longs.toByteArray(_offset[0])), val);
    }
    });

    return Longs.toByteArray(_offset[0]);
  }

  @Override
  public byte[] stream(byte[] colFam, byte[] key) {
    if (db.containsKey(Unpooled.buffer().writeBytes(colFam))) {
      if (db.get(Unpooled.buffer().writeBytes(colFam)).containsKey(Unpooled.buffer().writeBytes(key))) {
        return db.get(Unpooled.buffer().writeBytes(colFam)).get(Unpooled.buffer().writeBytes(key));
      } else {
        log.error("No such key " + new String(key) + " in colFam " + new String(colFam));
      }
    } else {
      log.error("No such colFam " + new String(colFam));
    }
    return null;
  }

  @Override
  public void close() throws Exception {
    db.clear();
  }
}
