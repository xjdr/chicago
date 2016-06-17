package com.xjeffrose.chicago;

import com.google.common.primitives.Ints;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.Env;
import org.rocksdb.HashLinkedListMemTableConfig;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;
import org.rocksdb.util.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBManager {
  private static final Logger log = LoggerFactory.getLogger(DBManager.class);

  private final Options options = new Options();
  private final ReadOptions readOptions = new ReadOptions();
  private final WriteOptions writeOptions = new WriteOptions();
  private final Map<String, ColumnFamilyHandle> columnFamilies = new ConcurrentHashMap<>();
  private final ChiConfig config;
  private final String delimeter = "@@@";
  private final HashMap<String,AtomicInteger> counter = new HashMap<>();


  private RocksDB db;

  public DBManager(ChiConfig config) {
    this.config = config;
    RocksDB.loadLibrary();
    configOptions();
    configReadOptions();
    configWriteOptions();

    try {
      File f = new File(config.getDBPath());
      if (f.exists() && !config.isGraceFullStart()) {
        removeDB(f);
      } else if (!f.exists()) {
        f.mkdir();
      }
      this.db = RocksDB.open(options, config.getDBPath());
    } catch (RocksDBException e) {
      log.error("Could not load DB: " + config.getDBPath() + " " + e.getMessage());
      System.exit(-1);
    }
  }

  void removeDB(File file) {
    File[] contents = file.listFiles();
    if (contents != null) {
      for (File f : contents) {
        removeDB(f);
      }
    }
    file.delete();
  }

  private void configOptions() {
    Env env = Env.getDefault();
    env.setBackgroundThreads(20);

    options
        .createStatistics()
        .setCreateIfMissing(true)
        .setWriteBufferSize(8 * SizeUnit.KB)
        .setMaxWriteBufferNumber(3)
        .setMaxBackgroundCompactions(10)
        //.setCompressionType(CompressionType.SNAPPY_COMPRESSION)
        .setEnv(env);
    if(!config.isDatabaseMode()){
      options.setCompactionStyle(CompactionStyle.FIFO)
             .setMaxTableFilesSizeFIFO(config.getCompactionSize());
    }


    options.setMemTableConfig(
        new HashLinkedListMemTableConfig()
            .setBucketCount(100000));


  }

  private void configReadOptions() {
    readOptions.setFillCache(false);
  }

  private void configWriteOptions() {
    writeOptions.setSync(true);
    writeOptions.setDisableWAL(true);
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
    if (colFamilyExists(name)){
      return true;
    }
    ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
    if(!config.isDatabaseMode()){
      columnFamilyOptions.setCompactionStyle(CompactionStyle.UNIVERSAL)
        .setMaxTableFilesSizeFIFO(config.getCompactionSize())
        .setDisableAutoCompactions(false);
    }

    ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(name, columnFamilyOptions);

    try {
      columnFamilies.put(new String(name), db.createColumnFamily(columnFamilyDescriptor));
      counter.put(new String(name), new AtomicInteger(0));
      return true;
    } catch (RocksDBException e) {
      log.error("Could not create Column Family: " + new String(name), e);
      return false;
    }
  }

  boolean write(byte[] colFam, byte[] key, byte[] value) {
    if (key == null) {
      log.error("Tried to write a null key");
      return false;
    } else if (value == null) {
      log.error("Tried to write a null value");
      return false;
    } else if (!colFamilyExists(colFam)) {
      synchronized (columnFamilies) {
        createColumnFamily(colFam);
      }
    }
    try {
      db.put(columnFamilies.get(new String(colFam)), writeOptions, key, value);
      return true;
    } catch (RocksDBException e) {
      log.error("Error writing record: " + new String(key), e);
      return false;
    }
  }

  byte[] read(byte[] colFam, byte[] key) {
    if (key == null) {
      log.error("Tried to read a null key");
      return null;
    } else {
      try {
        byte[] res = db.get(columnFamilies.get(new String(colFam)), readOptions, key);
        return res;
      } catch (RocksDBException e) {
        log.error("Error getting record: " + new String(key), e);
        return null;
      }
    }
  }

  boolean delete(byte[] colFam, byte[] key) {
    if (key == null) {
      log.error("Tried to delete a null key");
      return false;
    } else {
      try {
        db.remove(columnFamilies.get(new String(colFam)), key);
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

  public void destroy() {
    db.close();
  }

  public byte[] tsWrite(byte[] colFam, byte[] key, byte[] value){
    if (key == null) {
      log.error("Tried to write a null key");
      return null;
    } else if (value == null) {
      log.error("Tried to write a null value");
      return null;
    } else if (!colFamilyExists(colFam)) {
      synchronized (columnFamilies) {
        createColumnFamily(colFam);
      }
    }
    try {
      //Insert Key/Value only if it does not exists.
      if(!db.keyMayExist(readOptions,columnFamilies.get(new String(colFam)),key, new StringBuffer())){
        //Set the AtomicInteger for the colFam if the key is bigger than the already set value.
        if(Ints.fromByteArray(key) > counter.get(new String(colFam)).get()) {
          counter.get(new String(colFam)).set(Ints.fromByteArray(key) + 1);
        }
        log.info("Putting colFam/key/value : " +new String(colFam) + Ints.fromByteArray(key) + "/" + new String(value));
        db.put(columnFamilies.get(new String(colFam)), writeOptions, key, value);
      }
      return key;
    } catch (RocksDBException e) {
      log.error("Error writing record: " + new String(key), e);
      return null;
    }
  }

  public byte[] tsWrite(byte[] colFam, byte[] value) {
    if (value == null) {
      log.error("Tried to ts write a null value");
      return null;
    } else if (!colFamilyExists(colFam)) {
      createColumnFamily(colFam);
    }
    try {
      byte[] ts = Ints.toByteArray(counter.get(new String(colFam)).getAndIncrement());
      log.info("Putting key/value : "+ Ints.fromByteArray(ts)+"/"+new String(value));
      db.put(columnFamilies.get(new String(colFam)), writeOptions, ts, value);
      return ts;
    } catch (RocksDBException e) {
      log.error("Error writing record: " + new String(colFam), e);
      return null;
    }
  }

  public byte[] stream(byte[] colFam) {
    byte[] offset = new byte[]{};
    return stream(colFam, offset);
  }

  public byte[] stream(byte[] colFam, byte[] offset) {
    if (colFamilyExists(colFam)) {
      RocksIterator i = db.newIterator(columnFamilies.get(new String(colFam)), readOptions);
      ByteBuf bb = Unpooled.buffer();

      if (offset.length == 0) {
        i.seekToFirst();
      } else {
        i.seek(offset);
      }

      while (i.isValid()) {
        byte[] v = i.value();
        byte[] _v = new byte[v.length + 1];
        System.arraycopy(v, 0, _v, 0, v.length);
        System.arraycopy(new byte[]{'\0'}, 0, _v, v.length, 1);
        bb.writeBytes(_v);
        i.next();
      }

      i.seekToLast();

      bb.writeBytes(delimeter.getBytes());
      bb.writeBytes(i.key());

      return bb.array();
    } else {
      return null;
    }
  }

  public List<String> getColFams(){
    return new ArrayList<>(columnFamilies.keySet());
  }

  public List<byte[]> getKeys(byte[] colFam, byte[] offset){
    RocksIterator i = db.newIterator(columnFamilies.get(new String(colFam)), readOptions);
    List<byte[]> keySet = new ArrayList();
    if (offset.length == 0) {
      i.seekToFirst();
    } else {
      i.seek(offset);
    }

    while (i.isValid()) {
      keySet.add(i.key());
      i.next();
    }
    return keySet;
  }
}
