package com.adityamishra.mapreduce.engine;

import com.adityamishra.mapreduce.api.Context;
import com.adityamishra.mapreduce.api.KeyValue;
import com.adityamishra.mapreduce.api.Mapper;
import com.adityamishra.mapreduce.api.Reducer;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class LocalMapReduce<K1, V1, K2, V2, K3, V3> {
  private final Mapper<K1, V1, K2, V2> mapper;
  private final Reducer<K2, V2, K3, V3> reducer;
  private final Comparator<K2> keyComparator;

  public LocalMapReduce(Mapper<K1, V1, K2, V2> mapper, Reducer<K2, V2, K3, V3> reducer) {
    this(mapper, reducer, null);
  }

  public LocalMapReduce(Mapper<K1, V1, K2, V2> mapper, Reducer<K2, V2, K3, V3> reducer, Comparator<K2> keyComparator) {
    this.mapper = mapper;
    this.reducer = reducer;
    this.keyComparator = keyComparator;
  }

  private List<KeyValue<K2, V2>> mapPhase(List<KeyValue<K1, V1>> input) {
    Context<K2, V2> ctx = new Context<>();
    for (KeyValue<K1, V1> record : input) {
      mapper.map(record.getKey(), record.getValue(), ctx);
    }
    return ctx.read();
  }

  private Map<K2, List<V2>> shuffle(List<KeyValue<K2, V2>> intermediate) {
    Map<K2, List<V2>> grouped;
    if (keyComparator == null) {
      grouped = new LinkedHashMap<>();
    } else {
      grouped = new TreeMap<>(keyComparator);
    }
    for (KeyValue<K2, V2> kv : intermediate) {
      grouped.computeIfAbsent(kv.getKey(), key -> new ArrayList<>()).add(kv.getValue());
    }
    return grouped;
  }

  private List<KeyValue<K3, V3>> reducePhase(Map<K2, List<V2>> grouped) {
    Context<K3, V3> ctx = new Context<>();
    for (Map.Entry<K2, List<V2>> entry : grouped.entrySet()) {
      reducer.reduce(entry.getKey(), entry.getValue(), ctx);
    }
    return ctx.read();
  }

  public List<KeyValue<K3, V3>> run(List<KeyValue<K1, V1>> input) {
    List<KeyValue<K2, V2>> intermediate = mapPhase(input);
    Map<K2, List<V2>> grouped = shuffle(intermediate);
    List<KeyValue<K3, V3>> output = reducePhase(grouped);
    return output;
  }
}
