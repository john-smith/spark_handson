package com.neetomo.spark_handson;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Parallelize {
  public static void main(String[] args) {
    // Sparkを使うための変数の初期化 
    SparkConf conf = new SparkConf().setAppName("Parallelize").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);
    
    // JavaのListをSparkのRDDに変換
    List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    JavaRDD<Integer> distData = sc.parallelize(data);
    
    // countメソッドで行数の取得
    long count = distData.count();
    
    //結果の取得
    System.out.println(count);
    
    // 後処理
    sc.close();
  }
}
