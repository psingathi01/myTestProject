package com.myspark.java;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class FilterRDD2 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		SparkConf conf=new SparkConf().setAppName("filerRdd").setMaster("local");
		JavaSparkContext jsc=new JavaSparkContext(conf);
		
		List<Integer> list = new ArrayList<Integer>();
		
		list.add(1);
		list.add(2);
		list.add(3);
		list.add(4);
		list.add(5);
		list.add(6);
		
		JavaRDD<Integer> ints= jsc.parallelize(list);
		
		JavaRDD<Integer> fil=ints.filter(num -> num%3==0);
		
		fil.foreach(f ->{
			System.out.println(f);
		});

	}

}
