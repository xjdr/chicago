package com.xjeffrose.chicago;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChiUtil {
  private static final Logger log = LoggerFactory.getLogger(ChiUtil.class.getName());
  public static byte[] getTimeStamp() {
    return ZonedDateTime
        .now(ZoneId.of("UTC"))
        .format(DateTimeFormatter.RFC_1123_DATE_TIME).getBytes();
  }
}
