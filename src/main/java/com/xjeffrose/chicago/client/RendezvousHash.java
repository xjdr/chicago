package com.xjeffrose.chicago.client;

import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import java.util.Collection;
import java.util.concurrent.ConcurrentSkipListSet;

public class RendezvousHash<N extends Comparable<? super N>> {

  private final HashFunction hasher;
  private final Funnel<N> nodeFunnel;
  private final ConcurrentSkipListSet<N> ordered;

  public RendezvousHash(Funnel<N> nodeFunnel, Collection<N> init) {
    this.hasher = Hashing.murmur3_128();
    this.nodeFunnel = nodeFunnel;
    this.ordered = new ConcurrentSkipListSet<N>(init);
  }

  public boolean remove(N node) {
    return ordered.remove(node);
  }

  public boolean add(N node) {
    return ordered.add(node);
  }

  public N get(byte[] key) {
    long maxValue = Long.MIN_VALUE;
    N max = null;
    for (N node : ordered) {
      long nodesHash = hasher.newHasher()
          .putBytes(key)
          .putObject(node, nodeFunnel)
          .hash().asLong();
      if (nodesHash > maxValue) {
        max = node;
        maxValue = nodesHash;
      }
    }
    return max;
  }
}