package com.myspark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class AirPortsInUSA {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
		SparkConf sc = new SparkConf().setAppName("airPorts").setMaster("local");
		
		JavaSparkContext jsc = new JavaSparkContext(sc);
		
		JavaRDD<String> usaAirP=jsc.textFile("C:\\spark\\data\\airports.txt");
		
		Function<String,Boolean> fun= new Function<String,Boolean>(){
			
			public Boolean call(String str){
				String spl[]=str.split(COMMA_DELIMITER);
				return spl[3].equalsIgnoreCase("\"United States\"");
			}
		};
		
		JavaRDD<String> usaAirPorts= usaAirP.filter(fun);
		
				
		usaAirPorts.foreach(str ->{
			System.out.println(str);
		});
		
		JavaRDD<String> usaAp=usaAirPorts.map(line -> {
			String li[]=line.split(COMMA_DELIMITER);
			return li[1]+","+li[2];
			
		});
		
		usaAp.foreach(str ->{
			System.out.println(str);
		});

	}

}
