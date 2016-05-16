package com.xjeffrose.chicago;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.StringWriter;

public class ConfigSerializer {
  public static String serialize(ChiConfig config) {
    ObjectMapper mapper = new ObjectMapper();
    StringWriter sw = new StringWriter();
    try {
      mapper.writeValue(sw, config);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return sw.toString();
  }

  public static ChiConfig deserialize(String json) throws IOException {
    final ObjectMapper mapper = new ObjectMapper();

    return mapper.readValue(json.getBytes(), ChiConfig.class);
  }
}
