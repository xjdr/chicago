package com.xjeffrose.chicago;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChiUtil {
  private static final Logger log = LoggerFactory.getLogger(ChiUtil.class.getName());
  public static final String delimiter = "@@@";
  public static final String defaultColFam = "chicago";
  public static final int MaxBufferSize = 100000;
  public static byte[] getTimeStamp() {
    return ZonedDateTime
        .now(ZoneId.of("UTC"))
        .format(DateTimeFormatter.RFC_1123_DATE_TIME).getBytes();
  }

  public static long findOffset(byte[] input) {
    if (input != null && input.length > 0 && new String(input).contains(delimiter)) {
      int lastIndex = findLastOffsetIndex(input,delimiter.getBytes());
      long offset = Longs.fromByteArray(Arrays.copyOfRange(input,lastIndex+1,input.length-1));
      //Incrementing by 1 because this offset is already processed.
      return offset;
    }else{
      return -1;
    }
  }

  public static int findLastOffsetIndex(byte[] input, byte[] delimiter){
    for(int i = input.length -1; i >= (0+delimiter.length-1)  ; --i) {
      boolean found = true;
      int k =i;
      for(int j = delimiter.length -1; j >=0 ; --j, --k) {
        if (input[k] != delimiter[j]) {
          found = false;
          break;
        }
      }
      if (found) return i;
    }
    return -1;
  }
}
