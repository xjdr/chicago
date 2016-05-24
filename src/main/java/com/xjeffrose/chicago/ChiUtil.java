package com.xjeffrose.chicago;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class ChiUtil {
  public static byte[] getTimeStamp() {
    return ZonedDateTime
        .now(ZoneId.of("UTC"))
        .format(DateTimeFormatter.RFC_1123_DATE_TIME).getBytes();
  }
}
