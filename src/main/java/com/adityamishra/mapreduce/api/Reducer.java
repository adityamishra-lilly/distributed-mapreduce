package com.adityamishra.mapreduce.api;

public interface Reducer<K1, V1, K2, V2> {
  void reduce(K1 key, Iterable<V1> count, Context<K2, V2> ctx);
}
