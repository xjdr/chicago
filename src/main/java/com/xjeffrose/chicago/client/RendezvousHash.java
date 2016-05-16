package com.xjeffrose.chicago.client;

import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.ConcurrentSkipListSet;

class RendezvousHash<N> {

  private final HashFunction hasher;
  private final Funnel<N> nodeFunnel;
  private final ConcurrentSkipListSet<N> ordered;

  RendezvousHash(Funnel<N> nodeFunnel, Collection<N> init) {
    this.hasher = Hashing.murmur3_128();
    this.nodeFunnel = nodeFunnel;
    this.ordered = new ConcurrentSkipListSet<N>(init);
  }

  boolean remove(N node) {
    return ordered.remove(node);
  }

  boolean add(N node) {
    return ordered.add(node);
  }

  N get(byte[] key) {
    //TODO(JR): May need to improve performance for a large cluster ( > 200 nodes)
    HashMap<Long, N> hashMap = new HashMap();

    ordered.stream()
        .forEach(xs -> {
          hashMap.put(hasher.newHasher()
              .putBytes(key)
              .putObject(xs, nodeFunnel)
              .hash().asLong(), xs);
        });

    return hashMap.get(hashMap.keySet().stream().max(Long::compare).get());
  }

  // For testing only, will be removed
  @Deprecated
  N getOld(byte[] key) {
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