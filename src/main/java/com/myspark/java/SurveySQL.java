package com.myspark.java;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SurveySQL {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//SparkConf conf= new SparkConf().setAppName("sql").setMaster("local");
		//JavaSparkContext jsc= new JavaSparkContext(conf);
		List<StructField> fields = new ArrayList<>(27);
	    fields.add(DataTypes.createStructField("timestamp", DataTypes.TimestampType, true));
	    fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
	    fields.add(DataTypes.createStructField("gender", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("country", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("state", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("self_employed", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("family_history", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("treatment", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("work_interfere", DataTypes.TimestampType, true));
	    fields.add(DataTypes.createStructField("no_employees", DataTypes.IntegerType, true));
	    fields.add(DataTypes.createStructField("remote_work", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("tech_company", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("benefits", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("care_options", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("wellness_program", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("seek_help", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("anonymity", DataTypes.TimestampType, true));
	    fields.add(DataTypes.createStructField("leave", DataTypes.IntegerType, true));
	    fields.add(DataTypes.createStructField("mental_health_consequence", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("phys_health_consequence", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("coworkers", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("supervisor", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("mental_health_interview", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("phys_health_interview", DataTypes.StringType, true));	    
	    fields.add(DataTypes.createStructField("mental_vs_physical", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("obs_consequence", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("comments", DataTypes.StringType, true));
	    
	    
	    StructType schema = DataTypes.createStructType(fields);
	    
	    
		String warehouseLocation = "file:///C:/working_spark_java/spark-warehouse";
		SparkSession spark = SparkSession
			      .builder()
			      .appName("Java Spark SQL basic example")
			      .master("local[*]")
			      .config("spark.sql.warehouse.dir", warehouseLocation)
			     // .enableHiveSupport()
			      .getOrCreate();
		
		Dataset<Row> df= spark.read() .format("csv")
				  .option("header", true)
				  .schema(schema)
				  .option("mode", "PERMISSIVE")
				  .option("inferSchema", false).csv("C://spark//data//survey.csv");
		 
				   df.take(2);
				   df.printSchema();
				   df.createOrReplaceTempView("survey");
				   
				   Dataset<Row>  resultsDF = spark.sql("SELECT gender, state FROM survey WHERE age > 30");
				    resultsDF.show(10);
				    Dataset<Row>  resultsDF2 = spark.sql("SELECT gender, treatment,tech_company FROM survey WHERE age > 30");
				    resultsDF2.show(10);

	}

}
