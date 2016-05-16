package com.xjeffrose.chicago.client;

import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
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

  List<N> get(byte[] key) {
    HashMap<Long, N> hashMap = new HashMap();
    List<N> nodeList = new ArrayList<>();

      ordered.stream()
          .filter(xs -> !nodeList.contains(xs))
          .forEach(xs -> {
            hashMap.put(hasher.newHasher()
                .putBytes(key)
                .putObject(xs, nodeFunnel)
                .hash().asLong(), xs);

    });

    for (int i = 0; i < 3; i++) {
      nodeList.add(hashMap.remove(hashMap.keySet().stream().max(Long::compare).get()));
    }

    return nodeList;
  }

}