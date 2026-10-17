package com.adityamishra.mapreduce.engine;

import com.adityamishra.mapreduce.api.Context;
import com.adityamishra.mapreduce.api.KeyValue;
import com.adityamishra.mapreduce.api.Mapper;
import com.adityamishra.mapreduce.api.Reducer;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class LocalMapReduceTest {

  // Inline WordCount so the test doesn't depend on other classes.
  private static final Mapper<Long, String, String, Integer> WORD_MAPPER = (key, line, ctx) -> {
    for (String word : line.split("\\s+")) {
      ctx.write(word, 1);
    }
  };

  private static final Reducer<String, Integer, String, Integer> SUM_REDUCER = (key, values, ctx) -> {
    int sum = 0;
    for (int v : values) {
      sum += v;
    }
    ctx.write(key, sum);
  };

  @Test
  void wordCountEndToEnd() {
    LocalMapReduce<Long, String, String, Integer, String, Integer> engine = new LocalMapReduce<>(WORD_MAPPER,
        SUM_REDUCER,
        Comparator.naturalOrder());

    List<KeyValue<Long, String>> input = List.of(
        new KeyValue<>(0L, "hello world"),
        new KeyValue<>(12L, "hello again"));

    List<KeyValue<String, Integer>> out = engine.run(input);

    assertEquals(3, out.size());
    assertEquals("again", out.get(0).getKey());
    assertEquals(1, out.get(0).getValue());
    assertEquals("hello", out.get(1).getKey());
    assertEquals(2, out.get(1).getValue());
    assertEquals("world", out.get(2).getKey());
    assertEquals(1, out.get(2).getValue());
  }

  @Test
  void withoutComparatorKeepsFirstSeenOrder() {
    LocalMapReduce<Long, String, String, Integer, String, Integer> engine = new LocalMapReduce<>(WORD_MAPPER,
        SUM_REDUCER);

    List<KeyValue<String, Integer>> out = engine.run(List.of(new KeyValue<>(0L, "zebra apple zebra")));

    assertEquals("zebra", out.get(0).getKey());
    assertEquals(2, out.get(0).getValue());
    assertEquals("apple", out.get(1).getKey());
  }

  @Test
  void callsMapperOncePerRecord() {
    AtomicInteger calls = new AtomicInteger();
    Mapper<Long, String, String, Integer> countingMapper = (k, v, ctx) -> {
      calls.incrementAndGet();
      ctx.write(v, 1);
    };

    LocalMapReduce<Long, String, String, Integer, String, Integer> engine = new LocalMapReduce<>(countingMapper,
        SUM_REDUCER);

    engine.run(List.of(
        new KeyValue<>(0L, "a"),
        new KeyValue<>(1L, "b"),
        new KeyValue<>(2L, "c")));

    assertEquals(3, calls.get());
  }

  @Test
  void mapperCanUseInputKey() {
    // Emits (lineNumber, 1): proves the input key actually reaches the mapper.
    Mapper<Long, String, Long, Integer> keyMapper = (k, v, ctx) -> ctx.write(k, 1);
    Reducer<Long, Integer, Long, Integer> sum = (k, values, ctx) -> {
      int s = 0;
      for (int v : values)
        s += v;
      ctx.write(k, s);
    };

    LocalMapReduce<Long, String, Long, Integer, Long, Integer> engine = new LocalMapReduce<>(keyMapper, sum,
        Comparator.naturalOrder());

    List<KeyValue<Long, Integer>> out = engine.run(List.of(
        new KeyValue<>(5L, "x"),
        new KeyValue<>(9L, "y")));

    assertEquals(5L, out.get(0).getKey());
    assertEquals(9L, out.get(1).getKey());
  }

  @Test
  void emptyInputProducesEmptyOutput() {
    LocalMapReduce<Long, String, String, Integer, String, Integer> engine = new LocalMapReduce<>(WORD_MAPPER,
        SUM_REDUCER);

    assertTrue(engine.run(List.of()).isEmpty());
  }
}