package com.xjeffrose.chicago.db;

import com.google.common.primitives.Longs;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EncryptedStorageProviderTest {
  EncryptedStorageProvider db = new EncryptedStorageProvider(new InMemDBImpl());


  @Test
  public void encryptAndDecrypt() throws Exception {
    byte[] enc = db.encrypt("foo".getBytes());
    byte[] dec = db.decrypt(enc);

    assertEquals("foo", new String(dec));
  }

  @Test
  public void readAndWrite() throws Exception {
    boolean writeResp = db.write("colFam".getBytes(), "key".getBytes(), "val".getBytes());
    byte[] readResp = db.read("colFam".getBytes(), "key".getBytes());

    assertTrue(writeResp);
    assertEquals("val", new String(readResp));
  }


  @Test
  public void tsWriteAndStream() throws Exception {
    long offset = Longs.fromByteArray(db.tsWrite("colFam".getBytes(), "val".getBytes()));
    byte[] resp = db.stream("colFam".getBytes(), Longs.toByteArray(offset));

    assertEquals(0, offset);
    assertEquals("val", new String(resp));
  }

  @Test
  public void batchWrite() throws Exception {

  }

}