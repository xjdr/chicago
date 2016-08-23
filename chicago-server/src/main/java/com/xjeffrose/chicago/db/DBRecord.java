package com.xjeffrose.chicago.db;

import lombok.Getter;
import lombok.Setter;

public class DBRecord {
  @Getter
  byte[] colFam;
  @Getter
  byte[] key;
  @Getter
  @Setter
  byte[] value;

  public DBRecord(byte[] colFam, byte[] key, byte[] value) {
    this.colFam = colFam;
    this.value = value;
    this.key = key;
  }


}
