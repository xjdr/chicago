package com.xjeffrose.chicago;

import com.google.common.primitives.Ints;
import com.xjeffrose.chicago.client.ChicagoClientException;
import com.xjeffrose.xio.SSL.XioSecurityHandlerImpl;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ChicagoMasterManager {
  private final AtomicBoolean isThereAMaster = new AtomicBoolean();
  private final AtomicBoolean amIMaster = new AtomicBoolean();
  private final Random markov = new Random();
  private final String whoami;
  private final int q;
  private final List<String> witnessList;
  private final ChiConfig config;
  private final EventLoopGroup evg;
  private final CountDownLatch latch = new CountDownLatch(1);
  private final List<Integer> ballotBox = new ArrayList<>();

  private int master = -1;

  ChicagoMasterManager(ChiConfig config) {

    this.config = config;
    this.whoami = config.getEBindIP();

    this.q = config.getQuorum();
    this.witnessList = config.getWitnessList();
    this.evg = new NioEventLoopGroup(2);

    evg.schedule(() -> {
      electLeader();
    }, 2000, TimeUnit.MILLISECONDS);
  }

  public int getMaster() {
    return master;
  }

  public void setMaster(int master) {
    this.master = master;
    isThereAMaster.set(true);
  }

  public void electLeader() {
    while (!isThereAMaster.get()) {
      castBallot();
      try {
        latch.await(2000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    log.info("||||||||||||||||||||||||||||||||||| "
        + witnessList.get(getMaster())
        + " selected as master by " + whoami);
  }

  private void castBallot() {

    byte[] op = Ints.toByteArray(1);
    byte[] ballot = Ints.toByteArray(markov.nextInt(q));

    ByteBuf bb = UnpooledByteBufAllocator.DEFAULT.buffer();

    bb.writeBytes(op);
    bb.writeBytes(ballot);

    for (String w : witnessList) {
      ChannelFuture cf = connect(new InetSocketAddress(w.split("\\:")[0], Integer.parseInt(w.split("\\:")[1])));
      if (cf.channel().isWritable()) {
        cf.channel().write(bb);
      } else {
        //TODO(JR): Do Something in the event we cant connect to a witness
      }
    }

  }

  private ChannelFuture connect(InetSocketAddress addr) {

    Bootstrap bootstrap = new Bootstrap();
    bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 500)
        .option(ChannelOption.SO_REUSEADDR, true)
        .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
        .option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32 * 1024)
        .option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024)
        .option(ChannelOption.TCP_NODELAY, true);
    bootstrap.group(new NioEventLoopGroup(2))
        .channel(NioSocketChannel.class)
        .handler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel channel) throws Exception {
            ChannelPipeline cp = channel.pipeline();
            cp.addLast(new XioSecurityHandlerImpl(true).getEncryptionHandler());
            //cp.addLast(new XioIdleDisconnectHandler(20, 20, 20));
            cp.addLast(new SimpleChannelInboundHandler<ByteBuf>() {
              @Override
              protected void channelRead0(ChannelHandlerContext ctx, ByteBuf byteBuf) throws Exception {


                byte[] _op = new byte[4];
                byteBuf.readBytes(_op);
                int op = Ints.fromByteArray(_op);

                switch (op) {
                  case 0:
                    // Req to start leader election
                    // This is an error
                  case 1:
                    // Cast ballot
                    // This is an error
                  case 2:
                    // Confirm Leader
                    byte[] _ballot = new byte[4];
                    byteBuf.readBytes(_ballot);
                    int ballot = Ints.fromByteArray(_ballot);

                    setMaster(ballot);
                  case 3:
                    // Who is Leader
                    // This is an error
                  default:
                }

                latch.countDown();

              }
            });
          }
        });

    return bootstrap.connect(addr);
  }
}
