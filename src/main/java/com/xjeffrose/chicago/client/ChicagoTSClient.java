package com.xjeffrose.chicago.client;

import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.xjeffrose.chicago.ChiUtil;
import com.xjeffrose.chicago.ChicagoMessage;
import com.xjeffrose.chicago.DefaultChicagoMessage;
import com.xjeffrose.chicago.Op;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ChicagoTSClient extends BaseChicagoClient {
  private static final Logger log = LoggerFactory.getLogger(ChicagoTSClient.class);

  public ChicagoTSClient(String zkConnectionString, int quorum) throws InterruptedException {
    super(zkConnectionString, quorum);
  }

  public ChicagoTSClient(String address) throws InterruptedException{
    super(address);
  }

  public ListenableFuture<byte[]> stream(byte[] key) throws ChicagoClientTimeoutException {
    return stream(key, null);
  }

  public ListenableFuture<byte[]> stream(byte[] key, byte[] offset) throws ChicagoClientTimeoutException {
    return _getData(key,offset,Op.STREAM);
  }

  public ListenableFuture<byte[]> read(byte[] key) throws ChicagoClientTimeoutException {
    return read(ChiUtil.defaultColFam.getBytes(), key);
  }

  public ListenableFuture<byte[]> read(byte[] key, byte[] offset) throws ChicagoClientTimeoutException {
    return _getData(key,offset,Op.READ);
  }

  public ListenableFuture<byte[]> _getData(byte[] key, byte[] offset, Op op) throws ChicagoClientTimeoutException{
    ListeningExecutorService executor = MoreExecutors.listeningDecorator(exe);
    CountDownLatch latch = new CountDownLatch(1);
    return executor.submit(() -> {
      final long startTime = System.currentTimeMillis();
      ConcurrentLinkedDeque<byte[]> responseList = new ConcurrentLinkedDeque<>();
      List<String> hashList = getEffectiveNodes(key);
      for (String node : hashList) {
        if (node == null) {
        } else {
          ChannelFuture cf = connectionPoolMgr.getNode(node);
          if (cf.channel().isWritable()) {
            exe.execute(() -> {
              UUID id = UUID.randomUUID();
              Listener listener = connectionPoolMgr.getListener(node);
              switch(op){
                case READ:
                  cf.channel().writeAndFlush(new DefaultChicagoMessage(id, op, key, offset, null));
                  break;
                case STREAM:
                  cf.channel().writeAndFlush(new DefaultChicagoMessage(id, op, key, null, offset));
                  break;
              }
              listener.addID(id);
              listener.registerCallback(id, new ResponseListener() {
                @Override public void onResponse(ChicagoMessage chicagoMessage, ChannelHandlerContext ctx) {
                  if(latch.getCount() > 0) {
                    log.debug("Got response from context " + ctx.channel());
                  }
                  if(responseList.isEmpty()) {
                    responseList.add(chicagoMessage.getVal());
                  }
                  latch.countDown();
                }
              });
            });
          }
        }
        if(latch.getCount() > 0) {
          Thread.sleep(5);
        }
      }

      try{
        //Wait for response from anyone server.
        latch.await(TIMEOUT, TimeUnit.MILLISECONDS);
      } catch ( InterruptedException e){
        log.error("Failed to get response from any server !!!");
        Thread.currentThread().interrupt();
        throw new ChicagoClientTimeoutException();
      }

      return responseList.stream().findFirst().orElse(null);
    });
  }

  public byte[] write(byte[] key, byte[] value) throws ChicagoClientTimeoutException, ChicagoClientException {
    try {
      if (TIMEOUT_ENABLED) {
        return _write(key, value, 0).get(TIMEOUT, TimeUnit.MILLISECONDS);
      } else {
        return _write(key, value, 0).get();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      throw new ChicagoClientTimeoutException();
    }

    return null;
  }

  public  ListenableFuture<byte[]> _write(byte[] key, byte[] value, int _retries) throws ChicagoClientTimeoutException, ChicagoClientException {
    return _write(null,key, value, 0);
  }

  public ListenableFuture<byte[]> _write(byte[] colFam, byte[] key, byte[] value) throws ChicagoClientTimeoutException, ChicagoClientException {
    return _write(colFam ,key, value, 0);
  }


  private ListenableFuture<byte[]> _write(byte[] colFam, byte[] key, byte[] value, int _retries) throws ChicagoClientTimeoutException, ChicagoClientException {
    final int retries = _retries;

    final ConcurrentLinkedDeque<byte[]> responseList = new ConcurrentLinkedDeque<>();
    final ConcurrentLinkedDeque<UUID> idList = new ConcurrentLinkedDeque<>();

    ListeningExecutorService executor = MoreExecutors.listeningDecorator(exe);
    return executor.submit(() -> {
        int dquorum = quorum;
        final long startTime = System.currentTimeMillis();
        List<String> hashList = getEffectiveNodes(key);
        dquorum = hashList.size();
        CountDownLatch latch = new CountDownLatch(dquorum);
        log.debug("Writing to "+hashList.toString());
        hashList.stream().parallel().forEach(node -> {
          if (node == null) {

          } else {
            try {
              ChannelFuture cf = connectionPoolMgr.getNode(node);
              if (cf.channel().isWritable()) {
                exe.execute(() -> {
                    UUID id = UUID.randomUUID();
                    Listener listener = connectionPoolMgr.getListener(node); // Blocking
                    if(colFam != null){
                      cf.channel().writeAndFlush(new DefaultChicagoMessage(id, Op.TS_WRITE, colFam, key, value));
                    }else {
                      cf.channel().writeAndFlush(new DefaultChicagoMessage(id, Op.TS_WRITE, key, null, value));
                    }
                    listener.addID(id);
                    idList.add(id);
                    listener.registerCallback(id, new ResponseListener() {
                       @Override public void onResponse(ChicagoMessage chicagoMessage, ChannelHandlerContext ctx) {
                         responseList.add(chicagoMessage.getVal());
                         latch.countDown();
                       }
                     });
                });
              }
            } catch (ChicagoClientTimeoutException e){
              log.error("Client Timeout During Write Operation: ", e);
            }
          }
        });

        try {
          //Wait for all the servers to respond
          latch.await(TIMEOUT, TimeUnit.MILLISECONDS);
        }catch (InterruptedException e){
          log.error("Write failed. Expected number of response :"+dquorum +
            ". Number of response received :"+responseList.size());
          Thread.currentThread().interrupt();
          throw new ChicagoClientTimeoutException();
        }

        if (responseList.stream().allMatch(b -> b != null)) {
          for(byte[] response : responseList){
            System.out.println("Responses :" + Longs.fromByteArray(response));
          }
          System.out.println("Total time for write : "+ (System.currentTimeMillis() - startTime) + "ms" );
          return responseList.getFirst();
        } else {
          System.out.println("Return value from different servers are not same. Trying to write again!!!");
          if (MAX_RETRY < retries) {
            if (TIMEOUT_ENABLED) {
              return _write(key, value, retries + 1).get(TIMEOUT, TimeUnit.MILLISECONDS);
            } else {
              return _write(key, value, retries + 1).get();
            }
          } else {
            throw new ChicagoClientException("Could not successfully complete a replicated write. Please retry the operation");
          }
        }
    });
  }

  public ByteBuf bufStream(String key, long offset) throws Exception{
    ByteBuf returnByteBuf = Unpooled.directBuffer();
    returnByteBuf.markWriterIndex();
    returnByteBuf.markReaderIndex();
    ListeningExecutorService executor = MoreExecutors.listeningDecorator(exe);
    long[] offsets = new long[1];
    offsets[0] = offset;
    executor.submit(() -> {
      try {
        ListenableFuture<byte[]> resp =  stream(key.getBytes(), Longs.toByteArray(offsets[0]));

        byte[] resultArray = resp.get();
        String result = new String(resultArray);
        long old = -1;
        while (true) {
          if (!result.contains(ChiUtil.delimiter)) {
            System.out.println("No delimiter present");
            System.out.println(result);
            break;
          }

          offsets[0] = ChiUtil.findOffset(resultArray);
          String data = (result.split(ChiUtil.delimiter)[0]);
          int count = data.length();
          returnByteBuf.writeBytes(data.getBytes());
          //for (String line : lines) {
          //  if (line.length() != 0) {
          //    returnByteBuf.writeBytes(line.getBytes());
          //    count++;
          //  }
          //}
          if (count > 0) {
            offsets[0] = offsets[0] + 1;
          }
          if (old != -1 && (old == offsets[0])) {
            Thread.sleep(500);
          }
          //if(returnByteBuf.writerIndex() > 900000){
          //  returnByteBuf.resetWriterIndex();
          //}
          //
          //if(returnByteBuf.readerIndex() > 900000){
          //  returnByteBuf.resetReaderIndex();
          //}

          ListenableFuture<byte[]> newresp = stream(key.getBytes(), Longs.toByteArray(offsets[0]));
          resultArray = newresp.get();
          result = new String(resultArray);
          old = offsets[0];
        }
      }catch (Exception e){
        e.printStackTrace();
      }
      return;
    });
    return returnByteBuf;
  }
}
