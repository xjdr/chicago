package com.xjeffrose.chicago;

import com.google.common.primitives.Ints;
import com.xjeffrose.chicago.client.ChicagoClientException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChicagoElectionHandler extends SimpleChannelInboundHandler<ByteBuf> {
  private static final Logger log = LoggerFactory.getLogger(ChicagoElectionHandler.class.getName());

  private final ChiConfig config;
  private final ChicagoMasterManager masterManager;
  private final List<Integer> ballotBox = new ArrayList<>();

  public ChicagoElectionHandler(ChiConfig config, ChicagoMasterManager masterManager) {

    this.config = config;
    this.masterManager = masterManager;
  }

  private void confirmLeader(ChannelHandlerContext ctx, int master) {
    byte[] myOp = Ints.toByteArray(2);
    byte[] myBallot = Ints.toByteArray(master);

    ByteBuf bb = UnpooledByteBufAllocator.DEFAULT.buffer();

    bb.writeBytes(myOp);
    bb.writeBytes(myBallot);
    ctx.write(bb);
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ByteBuf byteBuf) throws Exception {
    byte[] _op = new byte[4];
    byteBuf.readBytes(_op);
    int op = Ints.fromByteArray(_op);

    switch (op) {
      case 0:
        // Req to start leader election
        break;
      case 1:
        // Cast ballot
        byte[] _ballot = new byte[4];
        byteBuf.readBytes(_ballot);
        int ballot = Ints.fromByteArray(_ballot);
        ballotBox.add(ballot);

        log.info("---------++++++++++++++++++++------------" + ctx.channel().remoteAddress()
        + " Voted for " + ballot);


        if (ballotBox.size() == config.getQuorum()) {
          if (ballotBox.stream().allMatch(xs -> xs == ballot)) {
            confirmLeader(ctx, ballot);
            ballotBox.clear();
          }
        }
        break;
      case 2:
        // Confirm Leader
//        confirmLeader(ctx, masterManager.getMaster());
        break;
      case 3:
        // Who is Leader
        confirmLeader(ctx, masterManager.getMaster());
        break;
      default:
        break;

    }
  }
}
