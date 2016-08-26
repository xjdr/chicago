package com.xjeffrose.chicago.server;

import com.xjeffrose.chicago.ChicagoPaxosClient;
import com.xjeffrose.chicago.db.DBManager;
import com.xjeffrose.chicago.db.StorageProvider;
import com.xjeffrose.xio.application.Application;
import com.xjeffrose.xio.bootstrap.ApplicationBootstrap;

import com.xjeffrose.xio.pipeline.XioSslHttp1_1Pipeline;
import io.netty.channel.ChannelHandler;
import io.netty.util.internal.PlatformDependent;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBRouter implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(DBRouter.class);

  //TODO(JR): Make this concurrent to applow for parallel streams
  private final StorageProvider db;
  private final DBManager manager;
  private final ChannelHandler handler;
  private final ChicagoPaxosHandler chicagoPaxosHandler;
  private final Map<String, AtomicLong> offset;
  private final Map<String, Integer> q;
  private final Map<String, Map<String, Long>> sessionCoordinator;
  private final Map<String, AtomicInteger> qCount;

  private Application application;

  public DBRouter(StorageProvider db, ChicagoPaxosClient paxosClient) {
    this.db = db;
    this.manager = new DBManager(db);
    this.handler = new ChicagoDBHandler(manager, paxosClient);
    this.offset = PlatformDependent.newConcurrentHashMap();
    this.q = PlatformDependent.newConcurrentHashMap();
    this.sessionCoordinator = PlatformDependent.newConcurrentHashMap();
    this.qCount = PlatformDependent.newConcurrentHashMap();
    this.chicagoPaxosHandler = new ChicagoPaxosHandler(offset, q, sessionCoordinator, qCount);
  }

  private ChicagoServerPipeline buildDbPipeline() {
    return new ChicagoServerPipeline("db") {
      @Override
      public ChannelHandler getApplicationHandler() {
        return handler;
      }
    };
  }

  private ChicagoServerPipeline buildPaxosPipeline() {
    return new ChicagoServerPipeline("paxos") {
      @Override
      public ChannelHandler getApplicationHandler() {
        return chicagoPaxosHandler;
      }
    };
  }


  public void run() {
    manager.startAsync().awaitRunning();

    application = new ApplicationBootstrap("chicago.application")
        .addServer("admin", (bs) -> bs.addToPipeline(new XioSslHttp1_1Pipeline()))
        .addServer("stats", (bs) -> bs.addToPipeline(new XioSslHttp1_1Pipeline()))
        .addServer("db", (bs) -> bs.addToPipeline(buildDbPipeline()))
        .addServer("paxos", (bs) -> bs.addToPipeline(buildPaxosPipeline()))
        .build();
  }

  @Override
  public void close() throws IOException {
    application.close();
    manager.stopAsync().awaitTerminated();
  }

  public void stop() {
    try {
      close();
    } catch (IOException e) {
      //TODO(JR): Should we just force close here?
      log.error("Error while attempting to close", e);
      throw new RuntimeException(e);
    }
  }

  public InetSocketAddress getDBBoundInetAddress() {
    return application.instrumentation("db").boundAddress();
  }
}
