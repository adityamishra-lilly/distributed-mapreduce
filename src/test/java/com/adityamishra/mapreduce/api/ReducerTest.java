package com.adityamishra.mapreduce.api;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

public class ReducerTest {
  class TestReducer implements Reducer<String, Integer, String, Integer> {
    public void reduce(String word, Iterable<Integer> count, Context<String, Integer> ctx) {
      int total = 0;
      for (int c : count) {
        total += c;
      }
      ctx.write(word, total);
    }
  }

  @Test
  void emitsWordCount() {
    Reducer<String, Integer, String, Integer> reducer = new TestReducer();
    Context<String, Integer> ctx = new Context<>();

    reducer.reduce("hello", List.of(1, 1), ctx);

    var out = ctx.read();
    assertEquals(1, out.size());
    assertEquals("hello", out.get(0).getKey());
    assertEquals(2, out.get(0).getValue());
  }

}
