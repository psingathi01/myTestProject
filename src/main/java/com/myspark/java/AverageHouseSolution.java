package com.myspark.java;

import java.math.BigDecimal;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class AverageHouseSolution {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf=new SparkConf().setAppName("ho").setMaster("local");
		JavaSparkContext ctx=new JavaSparkContext(conf);
				JavaRDD<String> lines=ctx.textFile("C:\\spark\\data\\real_estate.csv");
		
		JavaRDD<String> cleanLines= lines.filter(line -> !line.contains("Bedrooms"));
		
		//JavaPairRDD<String,Tuple2<Integer,String>> pair=cleanLines.map(lne -> lne.split(",")[3],new Tuple2(1,lne.split(",")[2]));
		
		JavaPairRDD<String,Tuple2<Integer,Double>> pair=cleanLines.mapToPair
				(li ->new Tuple2<>(li.split(",")[3],new Tuple2<Integer,Double>(1, new BigDecimal((li.split(",")[2]).trim()).doubleValue())) );
		
		//pair.foreach( f -> System.out.println(f._1) );
		JavaPairRDD<String,Tuple2<Integer,Double>> result = pair.reduceByKey((v1, v2) -> {
            return new Tuple2(v1._1+v2._1 , v1._2+v2._2);
        });
		
		
		//System.out.println("result :"+result.take(10));
		List <Tuple2<String,Tuple2<Integer,Double>>> list= result.collect();
		
	/*	for (list.iterator()) {
			
		}*/
		
		JavaPairRDD<String,Double> avg=result.mapValues(v ->
				v._2/v._1);
		
		System.out.println("avg :"+avg.take(10));
		
		
		
	
	}
}
