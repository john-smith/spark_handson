package com.neetomo.spark_handson;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class DAGVisualize {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);
    
    JavaPairRDD<String, Integer> wordCount = sc.textFile("src/main/resources/README.md")
        .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
        .mapToPair(s -> new Tuple2<String, Integer>(s, 1))
        .reduceByKey((a, b) -> a + b);
    System.out.println(wordCount.toDebugString());
  }
}
