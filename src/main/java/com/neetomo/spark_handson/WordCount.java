package com.neetomo.spark_handson;


import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount {
  public static void main(String[] args) {
    // 初期化
    SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);
    
    // テキストファイルの読み込み
    JavaRDD<String> input = sc.textFile("src/main/resources/README.md");
    
    // スペースで分割することで単語を取得
    JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String input) throws Exception {
        return Arrays.asList(input.split(" ")).iterator();
      }
    });
    
    // (<単語>, <1>)のPairを生成
    JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(String input) throws Exception {
        return new Tuple2<String, Integer>(input, 1);
      }
    });
    
    // 同一単語をkeyとして、単語ごとに足しあわせていく
    JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer a, Integer b) throws Exception {
        return a + b;
      }
    });
    
    List<Tuple2<String, Integer>> result = counts.collect();
    for (Tuple2<String, Integer> i : result) {
      System.out.println(i._1 + "\t" + i._2);
    }
    sc.close();
  }
}
