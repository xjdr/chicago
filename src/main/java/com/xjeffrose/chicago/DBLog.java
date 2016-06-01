package com.xjeffrose.chicago;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedDeque;
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

  public ConcurrentLinkedDeque<Entry> entries = new ConcurrentLinkedDeque<>();
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

  public String toString() {
    return "entries: " + entries;
  }
}
