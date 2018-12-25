package com.myspark.java;

import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class ChTransactions {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf= new SparkConf().setAppName("chtrans").setMaster("local");
		JavaSparkContext jsc= new JavaSparkContext(conf);
		JavaRDD<String> lines=jsc.textFile("file:///C:/github-archive/ch04_data_transactions.txt");
		JavaRDD<String[]> line=lines.map(s -> s.split("#"));
		JavaPairRDD<Integer,String[]> custTran=line.mapToPair(lne1 -> new Tuple2<Integer,String[]>(new Integer(lne1[2]),lne1)); 
		
		Map<Integer,Long> map	=custTran.countByKey();
		for (Map.Entry<Integer,Long> entry : map.entrySet()) {
			  Integer key = entry.getKey();
			  Long value = entry.getValue();
			  System.out.println("key="+key+" "+"val="+value);
			  // do stuff
			}
		
		
	}

}
