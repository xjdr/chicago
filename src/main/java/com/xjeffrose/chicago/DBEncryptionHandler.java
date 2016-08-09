package com.xjeffrose.chicago;

import com.intel.chimera.cipher.Cipher;
import com.intel.chimera.cipher.CipherTransformation;
import com.intel.chimera.stream.CryptoInputStream;
import com.intel.chimera.stream.CryptoOutputStream;
import com.intel.chimera.utils.Utils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBEncryptionHandler {
  private static final Logger log = LoggerFactory.getLogger(DBEncryptionHandler.class.getName());

  private final byte[] key;
  private final byte[] iv;
  private final int bufferSize = 4096;
  private final Properties properties = new Properties();
  private Cipher cipher;

  public DBEncryptionHandler() {
    this.key = new byte[16];
    this.iv = new byte[16];

    configure();
  }

  public DBEncryptionHandler(byte[] key, byte[] iv) {
    this.key = key;
    this.iv = iv;

    configure();
  }

  private void configure() {
    properties.setProperty("chimera.crypto.cipher.classes", "com.intel.chimera.crypto.OpensslCipher");
    try {
      cipher = Utils.getCipherInstance(CipherTransformation.AES_CTR_NOPADDING, properties);
    } catch (IOException e) {
      log.error("Error while configuring the encryption handler: ", e);
    }
  }

  public byte[] encrypt(byte[] raw) throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    CryptoOutputStream cos = new CryptoOutputStream(os, cipher, bufferSize, key, iv);
    cos.write(raw);
    cos.flush();
    cos.close();

    return os.toByteArray();
  }

  public byte[] decrypt(byte[] encryptedData) throws IOException {
    final byte[] decryptedData = new byte[bufferSize];
    CryptoInputStream cis = new CryptoInputStream(new ByteArrayInputStream(encryptedData), cipher, bufferSize, key, iv);
    cis.read(decryptedData, 0, bufferSize);

    int padding = 0;

    for (int i = 0; i < bufferSize; i++) {
      if (decryptedData[i] == '\0') {
        padding = i;
        break;
      }
    }

    byte[] result = new byte[padding];
    System.arraycopy(decryptedData, 0, result, 0, padding);

    return result;
  }
}
