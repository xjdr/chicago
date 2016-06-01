import ch.qos.logback.classic.filter.ThresholdFilter

appender("CONSOLE", ConsoleAppender) {
  withJansi = true

  filter(ThresholdFilter) {
    level = DEBUG
  }
  encoder(PatternLayoutEncoder) {
    pattern = "%-4relative [%thread] %-5level %logger{30} - %msg%n"
    outputPatternAsHeader = false
  }
}

logger("com.xjeffrose.xio.server.XioServerTransport", ERROR, ["CONSOLE"])
logger("org.apache.zookeeper.ClientCnxn", ERROR, ["CONSOLE"])
logger("org.apache.zookeeper.ClientCnxnSocketNIO", ERROR, ["CONSOLE"])

root(ERROR, ["CONSOLE"])