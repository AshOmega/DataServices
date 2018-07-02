package com.dataanalytics.sparkpackage;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkInit {

	static SparkSession spark = SparkSession.builder()
											.appName("DataServiceSpark")
											.master("local[12]")
											.config("spark.hadoop.validateOutputSpecs", "false")
											.getOrCreate();

	static Dataset<Row> df = spark.read()
								  .option("inferSchema", "true")
								  .option("header", "true")
								  .option("delimiter", ",")
								  .csv("C:\\Work\\Spark\\Test\\DataServices\\TA_restaurants_curated.csv");

	private SparkInit() {

	}

	public static SparkSession getInstance() {
		return spark;
	}

	public static Dataset<Row> getDF() {
		df.createOrReplaceTempView("df_data");
		return df;
	}
}
