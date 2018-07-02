package com.dataservice.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.dataanalytics.sparkpackage.SparkInit;
import com.datautils.PerformanceUtil;

@RestController
public class AppController {

	static List<String> stringList = new ArrayList<>();
	static List<Row> rowList = new ArrayList<>();
	private static final Logger log = LoggerFactory.getLogger(AppController.class);
	private Map<String, Dataset<Row>> locationMap = new HashMap<>();
	
@GetMapping(value = "/")
public String helloWorld()
{
	return "DataService application default";
}

@GetMapping(value = "/SparkTestRun", produces = {MediaType.APPLICATION_JSON_VALUE})
public List<String> sparkTestRun()
{
	SparkSession spark = SparkInit.getInstance();
		
	Dataset<Row> df  = SparkInit.getInstance().read()
				.option("inferSchema", "true")
				.option("header", "true")
				.option("delimiter", ",")
				.csv("C:\\Work\\Spark\\Test\\DataServices\\DB\\"+ "Berlin" + ".csv");
	
	stringList.clear();
	df.foreach( e -> {
		String val = e.getAs("Name").toString();
		stringList.add(val);
	});
	
	return stringList;
}

@GetMapping(value = "/splitData")
public void splitData()
{
	Dataset<Row> df = SparkInit.getDF();
	String[] areas = {"Amsterdam", "Athens", "Barcelona", "Berlin", "Bratislava", "Brussels", "Budapest", "Copenhagen", "Dublin", "Edinburgh", "Geneva", 
					"Hamburg", "Helsinki", "Krakow", "Lisbon", "Ljubljana", "London", "Luxembourg", "Lyon", "Madrid", "Milan", "Munich", "Oporto", "Oslo",
					"Paris", "Prague", "Rome", "Stockholm", "Vienna", "Warsaw", "Zurich"};
	
	for(String entry: areas)
	{
	 Dataset<Row> tempDf = df.select("*").where(df.col("City").equalTo(entry));
	 tempDf.coalesce(1).write().mode(SaveMode.Overwrite).option("inferSchema","true").option("header","true").csv("C:\\Work\\Spark\\Test\\DataServices\\DB\\" + entry);
	}
}

@GetMapping(value = "/fetchCustomerInfo", params = {"customer", "location"}, produces = {MediaType.APPLICATION_JSON_VALUE})
public List<?> fetchCustomerInfo(@RequestParam String customer, @RequestParam String location)
{
	PerformanceUtil obj = new PerformanceUtil();
	obj.startClock();
	 Dataset<Row> dfAll = SparkInit.getInstance().read()
				.option("inferSchema", "true")
				.option("header", "true")
				.option("delimiter", ",")
				.csv("C:\\Work\\Spark\\Test\\DataServices\\DB\\*.csv");
	
	 dfAll.persist(StorageLevel.MEMORY_ONLY_2());
	 
	 Dataset<Row> dfRegional = null;
	 
	 if(location.equals("all"))
	 {
		 Dataset<Row> newDf = dfAll.filter(dfAll.col("Name").contains(customer)); 
		stringList.clear();
		newDf.foreach( e -> {
			String val = e.getAs("Name").toString() + "--> " + e.getAs("City").toString();
			stringList.add(val);
		});
	 }
	 else
	 {
		 if(locationMap.getOrDefault(location, null)== null)
		 {
			 dfRegional  = SparkInit.getInstance().read()
						.option("inferSchema", "true")
						.option("header", "true")
						.option("delimiter", ",")
						.csv("C:\\Work\\Spark\\Test\\DataServices\\DB\\"+ location + ".csv");
		 }
		 else
		 {
			 dfRegional = locationMap.getOrDefault(location, null);
		 }
		 
		 if(dfRegional != null)
		 {
		  locationMap.put(location, dfRegional);
		  dfRegional.persist(StorageLevel.MEMORY_ONLY_2());
		 }
		 
		Dataset<Row> newDf = dfRegional.filter(dfRegional.col("City").equalTo(location)).filter(dfRegional.col("Name").contains(customer)); 
		stringList.clear();
		newDf.foreach( e -> {
			String val = e.getAs("Name").toString() + "--> " + e.getAs("City").toString();
			stringList.add(val);
		});
	 }
	 
	 obj.endClock("/fetchCustomerInfo");
	 
	return stringList;
}
}
