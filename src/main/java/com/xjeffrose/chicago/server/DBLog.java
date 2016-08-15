package com.xjeffrose.chicago.server;

import com.xjeffrose.chicago.Op;
import io.netty.util.internal.PlatformDependent;
import java.util.Deque;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBLog {
  public class Entry {
    Op op;
    String colFam;
    String key;
    String value;
    Entry(Op op, byte[] colFam, byte[] key, byte[] value) {
      this.op = op;
      this.colFam = new String(colFam);
      this.key = new String(key);
      this.value = new String(value);
    }
    Entry(Op op, byte[] colFam, byte[] key) {
      this.op = op;
      this.colFam = new String(colFam);
      this.key = new String(key);
      this.value = "";
    }
  }

  public Deque<Entry> entries = PlatformDependent.newConcurrentDeque();
  private static final Logger log = LoggerFactory.getLogger(DBLog.class.getName());

  public void addRead(byte[] colFam, byte[] key) {
    //log.error("addRead");
    entries.add(new Entry(Op.READ, colFam, key));
  }

  public void addWrite(byte[] colFam, byte[] key, byte[] value) {
    //log.error("addWrite");
    entries.add(new Entry(Op.WRITE, colFam, key, value));
  }

  public void addDelete(byte[] colFam, byte[] key) {
    //log.error("addDelete");
    entries.add(new Entry(Op.DELETE, colFam, key));
  }

  public void addScanColFamily() {
    entries.add(new Entry(Op.SCAN, null, null));
  }

  public String toString() {
    return "entries: " + entries;
  }
}
