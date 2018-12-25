package com.myspark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AirPortsInUSA2 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
		SparkConf conf= new SparkConf().setAppName("airport").setMaster("local");
		JavaSparkContext jsc=new JavaSparkContext(conf);
		
		JavaRDD<String> lines=jsc.textFile("C:\\spark\\data\\airports.txt");
		
		JavaRDD<String> usaAirports=lines.filter(line -> {
			String str=line.split(COMMA_DELIMITER)[3];
			return str.equalsIgnoreCase("\"United States\"");
		});
		
		JavaRDD<String> usaCities=usaAirports.map(line -> {
			String []str=line.split(COMMA_DELIMITER);
			return str[1]+","+str[2];
		});
		
		usaCities.collect().forEach(str ->{
			System.out.println(str);
		});
		
		
		
		

	}

}
