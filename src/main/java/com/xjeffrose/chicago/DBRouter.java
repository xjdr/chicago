package com.xjeffrose.chicago;

import com.google.common.collect.ImmutableMap;
import com.xjeffrose.xio.SSL.XioSecurityHandlerImpl;
import com.xjeffrose.xio.core.XioAggregatorFactory;
import com.xjeffrose.xio.core.XioCodecFactory;
import com.xjeffrose.xio.core.XioNoOpHandler;
import com.xjeffrose.xio.core.XioNoOpSecurityFactory;
import com.xjeffrose.xio.core.XioRoutingFilterFactory;
import com.xjeffrose.xio.core.XioSecurityFactory;
import com.xjeffrose.xio.core.XioSecurityHandlers;
import com.xjeffrose.xio.processor.XioProcessor;
import com.xjeffrose.xio.processor.XioProcessorFactory;
import com.xjeffrose.xio.server.XioBootstrap;
import com.xjeffrose.xio.server.XioServerConfig;
import com.xjeffrose.xio.server.XioServerDef;
import com.xjeffrose.xio.server.XioServerDefBuilder;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.http.HttpServerCodec;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBRouter implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(DBRouter.class);

  //TODO(JR): Make this concurrent to applow for parallel streams
  private final Set<XioServerDef> serverDefSet = new HashSet<>();
  private final ChiConfig config;
  private final DBManager dbManager;
  private XioBootstrap x;
  private final DBLog dbLog;

  public DBRouter(ChiConfig config, DBManager dbManager, DBLog dbLog) {
    this.config = config;
    this.dbManager = dbManager;
    this.dbLog = dbLog;
    config.setDbRouter(this);
  }

  private void configureAdminServer() {
    XioServerDef adminServer = new XioServerDefBuilder()
        .name("Chicago Server")
        .listen(new InetSocketAddress(config.getAdminBindIP(), config.getAdminPort()))
        .withSecurityFactory(new XioSecurityFactory() {
          @Override
          public XioSecurityHandlers getSecurityHandlers(XioServerDef xioServerDef, XioServerConfig xioServerConfig) {
            return new XioSecurityHandlerImpl(config.getCert(), config.getKey());
          }

          @Override
          public XioSecurityHandlers getSecurityHandlers() {
            return new XioSecurityHandlerImpl(config.getCert(), config.getKey());
          }
        })
        .withProcessorFactory(new XioProcessorFactory() {
          @Override
          public XioProcessor getProcessor() {
            return new ChicagoAdminProcessor();
          }
        })
        .withCodecFactory(new XioCodecFactory() {
          @Override
          public ChannelHandler getCodec() {
            return new HttpServerCodec();
          }
        })
        .withAggregator(new XioAggregatorFactory() {
          @Override
          public ChannelHandler getAggregator() {
            return null;
          }
        })
        .withRoutingFilter(new XioRoutingFilterFactory() {
          @Override
          public ChannelInboundHandler getRoutingFilter() {
            return null;
          }
        })
        .build();

    serverDefSet.add(adminServer);
  }

  private void configureStatsServer() {
    XioServerDef statsServer = new XioServerDefBuilder()
        .name("Chicago Server")
        .listen(new InetSocketAddress(config.getStatsBindIP(), config.getStatsPort()))
        .withSecurityFactory(new XioSecurityFactory() {
          @Override
          public XioSecurityHandlers getSecurityHandlers(XioServerDef xioServerDef, XioServerConfig xioServerConfig) {
            return new XioSecurityHandlerImpl(config.getCert(), config.getKey());
          }

          @Override
          public XioSecurityHandlers getSecurityHandlers() {
            return new XioSecurityHandlerImpl(config.getCert(), config.getKey());
          }
        })
        .withProcessorFactory(new XioProcessorFactory() {
          @Override
          public XioProcessor getProcessor() {
            return new ChicagoStatsProcessor();
          }
        })
        .withCodecFactory(new XioCodecFactory() {
          @Override
          public ChannelHandler getCodec() {
            return new HttpServerCodec();
          }
        })
        .withAggregator(new XioAggregatorFactory() {
          @Override
          public ChannelHandler getAggregator() {
            return null;
          }
        })
        .withRoutingFilter(new XioRoutingFilterFactory() {
          @Override
          public ChannelInboundHandler getRoutingFilter() {
            return null;
          }
        })
        .build();

    serverDefSet.add(statsServer);
  }

  private void configureDBServer() {
    XioServerDef dbServer = new XioServerDefBuilder()
        .name("Chicago Server")
        .listen(new InetSocketAddress(config.getDBBindIP(), config.getDBPort()))
//        .withSecurityFactory(new XioNoOpSecurityFactory())
        .withSecurityFactory(new XioSecurityFactory() {
          @Override
          public XioSecurityHandlers getSecurityHandlers(XioServerDef xioServerDef, XioServerConfig xioServerConfig) {
            return new XioSecurityHandlerImpl(config.getCert(), config.getKey());
          }

          @Override
          public XioSecurityHandlers getSecurityHandlers() {
            return new XioSecurityHandlerImpl(config.getCert(), config.getKey());
          }
        })
        .withProcessorFactory(new XioProcessorFactory() {
          @Override
          public XioProcessor getProcessor() {
            return new ChicagoProcessor();
          }
        })
        .withCodecFactory(new XioCodecFactory() {
          @Override
          public ChannelHandler getCodec() {
            return new ChicagoCodec();
          }
        })
        .withAggregator(new XioAggregatorFactory() {
          @Override
          public ChannelHandler getAggregator() {
            return new XioNoOpHandler();
          }
        })
        .withRoutingFilter(new XioRoutingFilterFactory() {
          @Override
          public ChannelInboundHandler getRoutingFilter() {
            return new ChicagoDBHandler(dbManager, dbLog);
          }
        })
        .build();

    serverDefSet.add(dbServer);
  }

  public void run() {

    configureAdminServer();
    configureStatsServer();
    configureDBServer();

    XioServerConfig serverConfig = XioServerConfig.newBuilder()
        .setBossThreadCount(config.getBossCount())
        .setBossThreadExecutor(Executors.newCachedThreadPool())
        .setWorkerThreadCount(config.getWorkers())
        .setWorkerThreadExecutor(Executors.newCachedThreadPool())
//        .setBootstrapOptions((Map<ChannelOption<Object>, Object>) new HashMap<>().put(ChannelOption.SO_KEEPALIVE, true))
        .build();

    ChannelGroup channels = new DefaultChannelGroup(new NioEventLoopGroup(config.getWorkers()).next());
    x = new XioBootstrap(serverDefSet, serverConfig, channels);

    try {
      x.start();
      config.setChannelStats(x.getXioMetrics());
      // For debug, leave commented out (or not, your choice if you like it)
      String msg = "--------------- Chicago Server Started!!! ----------------------";
      //System.out.println(msg);
      log.info(msg);
    } catch (Exception e) {
      e.printStackTrace();
      log.error("There was an error starting Chicago: ", e);
      x.stop();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    x.stop();
    dbManager.destroy();
  }

  public void stop() {
    try {
      close();
    } catch (IOException e) {
      //TODO(JR): Should we just force close here?
      log.error("Error while attempting to close", e);
      System.exit(-1);
    }
  }

}
