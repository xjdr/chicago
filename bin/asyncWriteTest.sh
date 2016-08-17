echo 'Running Chicago'
cd $(dirname $0)
java  -ea                         \
  $JAVA_OPTS                      \
  -Djava.net.preferIPv4Stack=true \
  -XX:+UseStringDeduplication     \
  -XX:+UseTLAB                    \
  -XX:+AggressiveOpts             \
  -XX:+UseParNewGC                \
  -XX:+UseConcMarkSweepGC         \
  -XX:+CMSParallelRemarkEnabled   \
  -XX:+CMSClassUnloadingEnabled   \
  -XX:ReservedCodeCacheSize=128m  \
  -XX:SurvivorRatio=128           \
  -XX:MaxTenuringThreshold=0      \
  -Xss16M                          \
  -Xms1024M                       \
  -Xmx16G                         \
  -server                         \
  -cp $HOME/src/xjdr/chicago/target/classes:$HOME/.m2/repository/com/xjeffrose/xio/0.12.0-SNAPSHOT/xio-0.12.0-SNAPSHOT.jar:$HOME/.m2/repository/com/google/guava/guava/19.0/guava-19.0.jar:$HOME/.m2/repository/io/netty/netty-all/4.1.0.Final/netty-all-4.1.0.Final.jar:$HOME/.m2/repository/io/netty/netty-tcnative-boringssl-static/1.1.33.Fork17/netty-tcnative-boringssl-static-1.1.33.Fork17.jar:$HOME/.m2/repository/org/apache/thrift/libthrift/0.9.3/libthrift-0.9.3.jar:$HOME/.m2/repository/com/typesafe/config/1.3.0/config-1.3.0.jar:$HOME/src/xjdr/chicago/3rdParty/rocksdbjni-4.8.0-osx.jar:$HOME/.m2/repository/org/apache/curator/curator-framework/2.9.1/curator-framework-2.9.1.jar:$HOME/.m2/repository/org/apache/curator/curator-client/2.9.1/curator-client-2.9.1.jar:$HOME/.m2/repository/org/apache/curator/curator-recipes/2.9.1/curator-recipes-2.9.1.jar:$HOME/.m2/repository/com/fasterxml/jackson/core/jackson-databind/2.7.3/jackson-databind-2.7.3.jar:$HOME/.m2/repository/com/fasterxml/jackson/core/jackson-core/2.7.3/jackson-core-2.7.3.jar:$HOME/.m2/repository/com/fasterxml/jackson/core/jackson-annotations/2.7.3/jackson-annotations-2.7.3.jar:$HOME/.m2/repository/org/apache/zookeeper/zookeeper/3.4.5/zookeeper-3.4.5.jar:$HOME/.m2/repository/org/slf4j/slf4j-api/1.6.1/slf4j-api-1.6.1.jar:$HOME/.m2/repository/org/slf4j/slf4j-log4j12/1.6.1/slf4j-log4j12-1.6.1.jar:$HOME/.m2/repository/log4j/log4j/1.2.15/log4j-1.2.15.jar:$HOME/.m2/repository/javax/mail/mail/1.4/mail-1.4.jar:$HOME/.m2/repository/javax/activation/activation/1.1/activation-1.1.jar:$HOME/.m2/repository/jline/jline/0.9.94/jline-0.9.94.jar:$HOME/.m2/repository/org/jboss/netty/netty/3.2.2.Final/netty-3.2.2.Final.jar:$HOME/.m2/repository/org/rocksdb/rocksdbjni/4.8.0/rocksdbjni-4.8.0.jar:$HOME/.m2/repository/org/projectlombok/lombok/1.16.8/lombok-1.16.8.jar:$HOME/.m2/repository/com/google/code/findbugs/jsr305/3.0.1/jsr305-3.0.1.jar:$HOME/.m2/repository/io/netty/netty-all/4.1.0.Final/netty-all-4.1.0.Final.jar:$HOME/.m2/repository/com/google/guava/guava/19.0/guava-19.0.jar:../target/chicago-0.4.0-SNAPSHOT.jar:../target/classes:../config com.xjeffrose.chicago.tools.WritePerformanceAsync
