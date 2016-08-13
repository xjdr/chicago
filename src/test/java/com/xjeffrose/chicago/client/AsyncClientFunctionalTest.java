package com.xjeffrose.chicago.client;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AsyncClientFunctionalTest {

  @Test
  public void readWriteHappyPath() throws Exception {

    ChicagoAsyncClient c = new ChicagoAsyncClient("10.24.25.188:2181,10.24.25.189:2181,10.25.145.56:2181,10.24.33.123:2181", 3);
    c.start();

    CountDownLatch l = new CountDownLatch(1);

    ListenableFuture<Boolean> w = c.write("testColFam".getBytes(), "testKey1".getBytes(), "testVal1".getBytes());

    Futures.addCallback(w, new FutureCallback<Boolean>() {
      @Override
      public void onSuccess(@Nullable Boolean aBoolean) {
        ListenableFuture<byte[]> r = c.read("testColFam".getBytes(), "testKey1".getBytes());
        Futures.addCallback(r, new FutureCallback<byte[]>() {
          @Override
          public void onSuccess(@Nullable byte[] bytes) {
            assertEquals("testVal1", new String(bytes));
            l.countDown();
          }

          @Override
          public void onFailure(Throwable throwable) {

          }
        });
      }

      @Override
      public void onFailure(Throwable throwable) {

      }
    });

    l.await(20000, TimeUnit.MILLISECONDS);
  }

  @Test
  public void tsWriteReadHappyPath() throws Exception {

    ChicagoAsyncClient c = new ChicagoAsyncClient("10.24.25.188:2181,10.24.25.189:2181,10.25.145.56:2181,10.24.33.123:2181", 3);
    c.start();

    CountDownLatch l = new CountDownLatch(1);

    ListenableFuture<byte[]> w = c.tsWrite("testTSColFam".getBytes(), "testTSVal1".getBytes());

    Futures.addCallback(w, new FutureCallback<byte[]>() {
      @Override
      public void onSuccess(@Nullable byte[] bites) {
        ListenableFuture<byte[]> r = c.stream("testTSColFam".getBytes(), bites);
        Futures.addCallback(r, new FutureCallback<byte[]>() {
          @Override
          public void onSuccess(@Nullable byte[] bytes) {
            assertEquals("testTSVal1", new String(bytes));
            l.countDown();
          }

          @Override
          public void onFailure(Throwable throwable) {

          }
        });
      }

      @Override
      public void onFailure(Throwable throwable) {

      }
    });

    l.await(20000, TimeUnit.MILLISECONDS);
  }


}
