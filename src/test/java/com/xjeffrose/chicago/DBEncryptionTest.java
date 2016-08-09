package com.xjeffrose.chicago;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

public class DBEncryptionTest {

  @Test
  public void encryptByteArray() throws Exception {

    DBEncryptionHandler encryptionHandler = new DBEncryptionHandler();

    String raw = "ShouldBeEncrypted";
    byte[] result;
    result = encryptionHandler.encrypt(raw.getBytes());

    assertNotNull(result);
    assertNotEquals(raw, new String(result));
  }

  @Test
  public void decryptByteArray() throws Exception {

    DBEncryptionHandler encryptionHandler = new DBEncryptionHandler();

    String raw = "ShouldBeEncrypted";
    byte[] enresult;
    enresult = encryptionHandler.encrypt(raw.getBytes());

    byte[] resp;
    resp = encryptionHandler.decrypt(enresult);

    assertEquals(raw, new String(resp));
  }

}
