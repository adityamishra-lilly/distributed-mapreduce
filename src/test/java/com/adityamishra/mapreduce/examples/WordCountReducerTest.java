package com.adityamishra.mapreduce.examples;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.adityamishra.mapreduce.api.Reducer;
import com.adityamishra.mapreduce.api.Context;

public class WordCountReducerTest {
  @Test
  void emitsWordCount() {
    Reducer<String, Integer, String, Integer> reducer = new WordCountReducer();
    Context<String, Integer> ctx = new Context<>();

    reducer.reduce("hello", List.of(1, 1), ctx);

    var out = ctx.read();
    assertEquals(1, out.size());
    assertEquals("hello", out.get(0).getKey());
    assertEquals(2, out.get(0).getValue());
  }
}
