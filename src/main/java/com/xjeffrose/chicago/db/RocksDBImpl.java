package com.xjeffrose.chicago.db;

import com.google.common.primitives.Longs;
import com.xjeffrose.chicago.ChiUtil;
import com.xjeffrose.chicago.ZkClient;
import com.xjeffrose.chicago.server.ChiConfig;
import com.xjeffrose.chicago.server.ChicagoServer;
import com.xjeffrose.chicago.server.DBRouter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.internal.PlatformDependent;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
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

public class RocksDBImpl implements AutoCloseable, StorageProvider {
  private static final Logger log = LoggerFactory.getLogger(RocksDBImpl.class);

  static {
    RocksDB.loadLibrary();
  }

  private final Options options = new Options();
  private final ReadOptions readOptions = new ReadOptions();
  private final WriteOptions writeOptions = new WriteOptions();
  private final Map<String, ColumnFamilyHandle> columnFamilies = PlatformDependent.newConcurrentHashMap();
  private final Map<String, AtomicLong> counter = PlatformDependent.newConcurrentHashMap();
  private ChiConfig config;
  private ZkClient zkClient;
  private RocksDB db;


  public RocksDBImpl(ChiConfig config) {
    this.config = config;
    configOptions();
    configReadOptions();
    configWriteOptions();


    File f = new File(config.getDbPath());
    if (f.exists() && !config.isGraceFullStart()) {
      removeDB(f);
    } else if (!f.exists()) {
      if (!f.mkdir()) {
        log.error("Unable to create DB");
      }
    }

    //createColumnFamily(ChiUtil.defaultColFam.getBytes());
  }

  public void setZkClient(ZkClient zkClient) {
    this.zkClient = zkClient;
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
        .setWriteBufferSize(1 * SizeUnit.GB)
        .setMaxWriteBufferNumber(3)
        .setMaxBackgroundCompactions(10)
        //.setCompressionType(CompressionType.SNAPPY_COMPRESSION)
        .setEnv(env);
    setCompactionOptions(options);

    options.setMemTableConfig(
        new HashLinkedListMemTableConfig()
            .setBucketCount(100000));
  }

  private void setCompactionOptions(Options options) {
    if (config == null) {

    } else if (!config.isDatabaseMode()) {
      options.setCompactionStyle(CompactionStyle.FIFO)
          .setMaxTableFilesSizeFIFO(config.getCompactionSize());
    }
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
      if (colFamilyExists(_name)) {
        db.dropColumnFamily(columnFamilies.get(name));
        columnFamilies.remove(name);
      }
      return true;
    } catch (RocksDBException e) {
      log.error("Could not delete Column Family: " + name, e);
      return false;
    }
  }

  private synchronized boolean createColumnFamily(byte[] name) {
    if (colFamilyExists(name)) {
      return true;
    }

    ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
    if (config == null) {
      columnFamilyOptions
          .setWriteBufferSize(1 * SizeUnit.GB)
          .setMemtablePrefixBloomProbes(1)
          .setMemTableConfig(new HashLinkedListMemTableConfig()
              .setBucketCount(100000));
    } else if (!config.isDatabaseMode()) {
      columnFamilyOptions.setCompactionStyle(CompactionStyle.FIFO)
          .setMaxTableFilesSizeFIFO(config.getCompactionSize())
          .setWriteBufferSize(1 * SizeUnit.GB)
          .setMemtablePrefixBloomProbes(1)
          .setMemTableConfig(new HashLinkedListMemTableConfig()
              .setBucketCount(100000));
    }

    ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(name, columnFamilyOptions);

    try {
      columnFamilies.put(new String(name), db.createColumnFamily(columnFamilyDescriptor));
      counter.put(new String(name), new AtomicLong(0));
      if (zkClient != null) {
        zkClient.createIfNotExist(ChicagoServer.NODE_LOCK_PATH + "/" + new String(name), "");
      }
      return true;
    } catch (RocksDBException e) {
      log.error("Could not create Column Family: " + new String(name), e);
      return false;
    }
  }

  @Override
  public boolean write(byte[] colFam, byte[] key, byte[] value) {
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

  @Override
  public byte[] read(byte[] colFam, byte[] key) {
    if (key == null) {
      log.error("Tried to read a null key");
      return null;
    } else {
      try {
        return db.get(columnFamilies.get(new String(colFam)), readOptions, key);
      } catch (RocksDBException e) {
        log.error("Error getting record: " + new String(key), e);
        return null;
      }
    }
  }

  public void resetIfOverflow(AtomicLong l, String colFam) {
    if (l.get() < 0 || l.get() == Long.MIN_VALUE) {
      l.set(0);
    }
  }

  public boolean delete(byte[] colFam) {
    try {
      if (colFamilyExists(colFam)) {
        log.info("Deleting the column Family :" + new String(colFam));
        ColumnFamilyHandle ch = columnFamilies.remove(new String(colFam));
        db.dropColumnFamily(ch);
        counter.remove(new String(colFam));
      }
    } catch (RocksDBException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  @Override
  public boolean delete(byte[] colFam, byte[] key) {
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
    try (RocksIterator i = db.newIterator(readOptions)) {
      List<byte[]> keySet = new ArrayList();
      i.seekToFirst();

      while (i.isValid()) {
        keySet.add(i.key());
        i.next();
      }

      return keySet;
    }
  }

  public void destroy() {
    db.close();
  }

  public byte[] tsWrite(byte[] colFam, byte[] key, byte[] value) {
    if (key == null) {
      log.error("Tried to write a null key");
      return null;
    } else if (value == null) {
      log.error("Tried to write a null value");
      return null;
    } else if (!colFamilyExists(colFam)) {
      createColumnFamily(colFam);
    }
    try {
      //Insert Key/Value only if it does not exists.
      if (!db.keyMayExist(readOptions, columnFamilies.get(new String(colFam)), key, new StringBuffer())) {
        //Set the AtomicInteger for the colFam if the key is bigger than the already set value.
        if (Longs.fromByteArray(key) > counter.get(new String(colFam)).get()) {
          counter.get(new String(colFam)).set(Longs.fromByteArray(key) + 1);
          resetIfOverflow(counter.get(new String(colFam)), new String(colFam));
        }
        if (Longs.fromByteArray(key) % 1000 == 0) {
          log.info("colFam/key reached : " + new String(colFam) + " " + Longs.fromByteArray(key));
        }
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
      byte[] ts = Longs.toByteArray(counter.get(new String(colFam)).getAndIncrement());
      if (Longs.fromByteArray(ts) % 1000 == 0) {
        log.info("key reached " + Longs.fromByteArray(ts) + " for colFam " + new String(colFam));
      }
      resetIfOverflow(counter.get(new String(colFam)), new String(colFam));
      db.put(columnFamilies.get(new String(colFam)), writeOptions, ts, value);

      return ts;
    } catch (RocksDBException e) {
      log.error("Error writing record: " + new String(colFam), e);
      return null;
    }
  }

  public byte[] batchWrite(byte[] colFam, byte[] value) {
    if (value == null) {
      log.error("Tried to ts write a null value");
      return null;
    } else if (!colFamilyExists(colFam)) {
      createColumnFamily(colFam);
    }
//    int noOfRecords = value.split(ChiUtil.delimiter).length;
//    long returnVal = counter.get(new String(colFam)).get() + noOfRecords;
    String[] values = new String(value).split(ChiUtil.delimiter);
    byte[] ts = new byte[0];
    for (String val : values) {
      ts = Longs.toByteArray(counter.get(new String(colFam)).getAndIncrement());
      if (Longs.fromByteArray(ts) % 1000 == 0) {
        log.info("key reached " + Longs.fromByteArray(ts) + " for colFam " + new String(colFam));
      }
      resetIfOverflow(counter.get(new String(colFam)), new String(colFam));
      try {
        db.put(columnFamilies.get(new String(colFam)), writeOptions, ts, val.getBytes());
      } catch (RocksDBException e) {
        log.error("Error writing record: " + new String(colFam), e);
        return null;
      }
    }
    return ts;
  }

  public List<DBRecord> stream(byte[] colFam) {
    byte[] offset = new byte[]{};
    return stream(colFam, offset);
  }

  public List<DBRecord> stream(byte[] colFam, byte[] offset) {
    long startTime = System.currentTimeMillis();
    List<DBRecord> values = new ArrayList<>();
    log.info("Requesting stream");
    if (colFamilyExists(colFam)) {
      try (RocksIterator i = db.newIterator(columnFamilies.get(new String(colFam)), readOptions)) {
        ByteBuf bb = Unpooled.buffer();
        byte[] lastOffset = Longs.toByteArray(0l);

        if (offset.length == 0) {
          i.seekToLast();
        } else {
          lastOffset = offset;
          i.seek(offset);
        }
        int size = 0;
        while (i.isValid() && size < ChiUtil.MaxBufferSize) {
          values.add(new DBRecord(colFam,i.key(),i.value()));
          lastOffset = i.key();
          size += colFam.length + i.key().length + i.value().length;
          i.next();
        }

        log.info("Stream response from DB : " + (System.currentTimeMillis() - startTime) + "ms with last offset as " + Longs.fromByteArray(lastOffset));
        return values;
      }
    } else {
      return null;
    }
  }

  public List<String> getColFams() {
    return new ArrayList<>(columnFamilies.keySet());
  }

  public List<byte[]> getKeys(byte[] colFam, byte[] offset) {
    try (RocksIterator i = db.newIterator(columnFamilies.get(new String(colFam)), readOptions)) {
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

  @Override
  public void close() {
    destroy();
  }

  @Override
  public void open() {
    try {
      this.db = RocksDB.open(options, config.getDbPath());
    } catch (RocksDBException e) {
      log.error("Unable to open RocksDB ", e);
    }
  }
}
