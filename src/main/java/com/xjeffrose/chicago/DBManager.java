package com.xjeffrose.chicago;

import org.apache.log4j.Logger;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class DBManager {
  private static final Logger log = Logger.getLogger(DBManager.class);

  private RocksDB db;

  public DBManager(ChiConfig config) {
    RocksDB.loadLibrary();
    Options options = new Options().setCreateIfMissing(true);

    try {
      this.db = RocksDB.open(options, config.getDBPath());
    } catch (RocksDBException e) {
      log.error("Could not load DB: " + config.getDBPath());
      System.exit(-1);
    }
  }

  public boolean write(byte[] key, byte[] value) {
    if (key == null) {
      log.error("Tried to write a null key");
      return false;
    } else if (value == null) {
      log.error("Tried to write a null value");
      return false;
    } else {
      try {
        db.put(key, value);
      } catch (RocksDBException e) {
        return false;
      }
    }

    return true;
  }

  public byte[] get(byte[] key) {
    if (key == null) {
      log.error("Tried to get a null key");
      return null;
    } else {
      try {
        return db.get(key);
      } catch (RocksDBException e) {
        return null;
      }
    }
  }

}
