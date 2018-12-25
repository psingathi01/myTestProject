package com.myspark.java;


import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCount2 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf=new SparkConf().setAppName("wcount").setMaster("local");
		
		JavaSparkContext jsc=new JavaSparkContext(conf);
		
		JavaRDD<String> lines=jsc.textFile("C:/myspark_java/src/main/resources/input.txt");
		
		JavaRDD<String> words=lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator() );
		
		JavaPairRDD<String,Integer> wordPair=words.mapToPair(word -> new Tuple2(word,1));
		
		JavaPairRDD<String,Integer> wordPairReduce=wordPair.reduceByKey((a,b) -> a+b);
		
		for( Tuple2<String,Integer> r:wordPairReduce.collect()){
			System.out.println(" "+r._1+" "+r._2);
		}
		
	}

}
