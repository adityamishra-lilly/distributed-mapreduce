package com.adityamishra.mapreduce.examples;

import com.adityamishra.mapreduce.api.Reducer;
import com.adityamishra.mapreduce.api.Context;

public class WordCountReducer implements Reducer<String, Integer, String, Integer> {
  public void reduce(String word, Iterable<Integer> count, Context<String, Integer> ctx) {
    int total = 0;
    for (int c : count) {
      total += c;
    }
    ctx.write(word, total);
  }
}