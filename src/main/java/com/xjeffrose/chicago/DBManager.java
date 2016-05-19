package com.xjeffrose.chicago;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.Env;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

class DBManager {
  private static final Logger log = Logger.getLogger(DBManager.class);

  private final Options options = new Options();
  private final ReadOptions readOptions = new ReadOptions();
  private final WriteOptions writeOptions = new WriteOptions();
  private final Map<String, ColumnFamilyHandle> columnFamilies = new HashMap<>();

  private RocksDB db;

  DBManager(ChiConfig config) {
    RocksDB.loadLibrary();

    configOptions();
    configReadOptions();
    configWriteOptions();

    try {
      this.db = RocksDB.open(options, config.getDBPath());
    } catch (RocksDBException e) {
      log.error("Could not load DB: " + config.getDBPath() + e.getMessage());
      System.exit(-1);
    }
  }

  private void configOptions() {
    options.setCreateIfMissing(true);

    Env env = Env.getDefault();
    env.setBackgroundThreads(20);

    options.setEnv(env);
  }

  private void configReadOptions() {
//    readOptions.setTailing(true);
  }

  private void configWriteOptions() {
//    writeOptions.setDisableWAL(true);
  }

  boolean colFamilyExists(byte[] name) {
    return columnFamilies.containsKey(new String(name));
  }

  boolean deleteColumnFamily(byte[] _name) {
    final String name = new String(_name);
    try {
      db.dropColumnFamily(columnFamilies.get(name));
      columnFamilies.remove(name);
      return true;
    } catch (RocksDBException e) {
      log.error("Could not delete Column Family: " + name, e);
      return false;
    }
  }

  private boolean createColumnFamily(byte[] name) {
    ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
    ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(name, columnFamilyOptions);

    try {
      columnFamilies.put(new String(name), db.createColumnFamily(columnFamilyDescriptor));
      return true;
    } catch (RocksDBException e) {
      log.error("Could not create Column Family: " + new String(name), e);
      return false;
    }
  }

  boolean write(byte[] key, byte[] value) {
    if (key == null) {
      log.error("Tried to write a null key");
      return false;
    } else if (value == null) {
      log.error("Tried to write a null value");
      return false;
    } else {
      try {
        db.put(writeOptions, key, value);
      } catch (RocksDBException e) {
        log.error("Error writing record: " + new String(key), e);
        return false;
      }
    }

    return true;
  }

  byte[] read(byte[] key) {
    if (key == null) {
      log.error("Tried to read a null key");
      return null;
    } else {
      try {
        return db.get(readOptions, key);
      } catch (RocksDBException e) {
        log.error("Error getting record: " + new String(key), e);
        return null;
      }
    }
  }

  boolean delete(byte[] key) {
    if (key == null) {
      log.error("Tried to delete a null key");
      return false;
    } else {
      try {
        db.remove(key);
        return true;
      } catch (RocksDBException e) {
        log.error("Error deleting record: " + new String(key), e);
        return false;
      }
    }
  }

  List<byte[]> getKeys(ReadOptions readOptions) {
    RocksIterator i = db.newIterator(readOptions);
    List<byte[]> keySet = new ArrayList();
    i.seekToFirst();

    while (i.isValid()) {
      keySet.add(i.key());
      i.next();
    }

    return keySet;
  }

  void destroy() {
    db.close();
  }

}
