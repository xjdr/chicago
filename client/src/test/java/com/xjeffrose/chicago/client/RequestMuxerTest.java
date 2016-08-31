package com.xjeffrose.chicago.client;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.xjeffrose.chicago.ChicagoMessage;
import com.xjeffrose.chicago.DefaultChicagoMessage;
import com.xjeffrose.chicago.Op;
import io.netty.channel.*;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * Created by smadan on 8/13/16.
 */
public class RequestMuxerTest {
  @Mock
  ChicagoConnector connector;
  @Mock
  ChannelHandler handler;
  RequestMuxer<ChicagoMessage> requestMuxer;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  public void setUp() throws Exception {
   requestMuxer = new RequestMuxer<>("127.0.0.1:12000",handler,new NioEventLoopGroup(5,
      new ThreadFactoryBuilder()
        .setNameFormat("chicagoClient-nioEventLoopGroup-%d")
        .build()
    ));
    requestMuxer.setConnector(connector);
  }

  @Test
  public void writeTest() throws Exception{
    SettableFuture<ChannelFuture> f = SettableFuture.create();
    ChannelFuture cf = mock(ChannelFuture.class);
    f.set(cf);
    when(connector.connect(new InetSocketAddress("127.0.0.1",12000))).thenReturn(f);
    when(cf.isSuccess()).thenReturn(true);

    Channel helper = new EmbeddedChannel(mock(ChannelHandler.class));
    Channel chMock = spy(helper);
    ChannelPromise promise = new DefaultChannelPromise(chMock);

    when(cf.channel()).thenReturn(helper);
    when(chMock.isWritable()).thenReturn(true);
    requestMuxer.start();
    ChicagoMessage cm = new DefaultChicagoMessage(UUID.randomUUID(), Op.TS_WRITE,"colFam".getBytes(),null,"val".getBytes());
    SettableFuture<Boolean> f2 = SettableFuture.create();
    DefaultChannelPromise cfmock = mock(DefaultChannelPromise.class);
    when(chMock.writeAndFlush(cm)).thenReturn(promise);
    promise.setSuccess();
    when(cfmock.isSuccess()).thenReturn(true);
    requestMuxer.write(cm, f2);
    CountDownLatch latch = new CountDownLatch(1);
    Futures.addCallback(f2, new FutureCallback<Boolean>() {
      @Override
      public void onSuccess(@Nullable Boolean result) {
        latch.countDown();
      }

      @Override
      public void onFailure(Throwable t) {

      }
    });

    latch.await();
  }

  @Test
  public void writeTestWithBadChannel() throws Exception{
    SettableFuture<ChannelFuture> f = SettableFuture.create();
    ChannelFuture cf = mock(ChannelFuture.class);
    f.set(cf);
    when(connector.connect(new InetSocketAddress("127.0.0.1",12000))).thenReturn(f);
    when(cf.isSuccess()).thenReturn(true);

    Channel helper = new EmbeddedChannel(mock(ChannelHandler.class));
    Channel chMock = spy(helper);
    ChannelPromise promise = new DefaultChannelPromise(chMock);
    promise.setFailure(new RuntimeException("Channel not acitve!!!"));

    when(cf.channel()).thenReturn(chMock);
    when(chMock.isWritable()).thenReturn(false);
    when(chMock.isActive()).thenReturn(false);
    requestMuxer.start();
    ChicagoMessage cm = new DefaultChicagoMessage(UUID.randomUUID(), Op.TS_WRITE,"colFam".getBytes(),null,"val".getBytes());
    SettableFuture<Boolean> f2 = SettableFuture.create();
    DefaultChannelPromise cfmock = mock(DefaultChannelPromise.class);
    when(chMock.write(cm)).thenReturn(promise);
    when(cfmock.isSuccess()).thenReturn(false);
    CountDownLatch latch = new CountDownLatch(1);
    Futures.addCallback(f2, new FutureCallback<Boolean>() {
      @Override
      public void onSuccess(@Nullable Boolean result) {
        latch.countDown();
      }

      @Override
      public void onFailure(Throwable t) {
        latch.countDown();
      }
    });


    requestMuxer.write(cm, f2);

    latch.await();
  }

}
