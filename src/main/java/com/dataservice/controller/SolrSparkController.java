package com.dataservice.controller;

import java.io.IOException;
import java.io.Serializable;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.dataanalytics.modelBeans.HotelModelBean;
import com.dataanalytics.sparkpackage.SparkInit;
import com.datautils.PerformanceUtil;

@RestController
@RequestMapping("/sparksolr")
public class SolrSparkController implements Serializable{
	
	private static final Logger log = LoggerFactory.getLogger(SolrSparkController.class);
	Long maxCount = 0L;
	Long currentCount = 0L;
	
	@GetMapping(value = "/")
	public String helloWorldSolr()
	{
		return "Solr application default";
	}
	
	@GetMapping(value = "/solrTestDelete")
	 public void solrTestDelete() throws SolrServerException, IOException { 
	      String urlString = "http://localhost:8983/solr/spark_hotel"; 
	      SolrClient Solr = new HttpSolrClient.Builder(urlString).build();   
	      
	      SolrInputDocument doc = new SolrInputDocument();    
	      Solr.deleteByQuery("*");       
	      Solr.commit(); 
	      log.info("Documents deleted"); 
	   } 
	
	@GetMapping(value = "/solrTestRead")
	 public SolrDocumentList solrTestRead(@RequestParam String q, @RequestParam Integer rows) throws SolrServerException, IOException { 
		
		  PerformanceUtil obj = new PerformanceUtil();
	      obj.startClock();
		
		  String urlString = "http://localhost:8983/solr/spark_hotel"; 
		  SolrClient Solr = new HttpSolrClient.Builder(urlString).build();  
	      
	      SolrQuery query = new SolrQuery();  
	      query.setQuery(q);  
	      query.addField("*");  
	      query.setRows(rows);
	   
	      QueryResponse queryResponse = Solr.query(query);  
	    
	      SolrDocumentList docs = queryResponse.getResults();    
	      log.info("Count =  {0}",  docs.size());
	      Solr.commit();         
	      obj.endClock("/solrTestRead");
	      return docs;
	   }

	
	@GetMapping(value = "/solrSparkWrite")
	 public String solrSparkWrite(@RequestParam String location) throws SolrServerException, IOException { 
	
	 SolrSparkController instance = new SolrSparkController();
	 Dataset<Row> dfAll  = SparkInit.getInstance().read()
				.option("inferSchema", "true")
				.option("header", "true")
				.option("delimiter", ",")
				.csv("C:\\Work\\Spark\\Test\\DataServices\\DB\\"+ location + ".csv");
	
	 dfAll.persist(StorageLevel.MEMORY_ONLY_2());
	 instance.maxCount = dfAll.count();
	 instance.currentCount = 0L;
	 dfAll.foreach(e -> {
		 					String name = e.getAs("Name") != null? e.getAs("Name").toString() : "nil";
		 					String city = e.getAs("City") != null? e.getAs("City").toString() : "nil";
		 					String ranking = e.getAs("Ranking") != null? e.getAs("Ranking").toString() : "0";
		 					String rating = e.getAs("Rating") != null? e.getAs("Rating").toString() : "0";
		 					String reviewCount = e.getAs("Number of Reviews") != null? e.getAs("Number of Reviews").toString() : "0";
		 					String url = e.getAs("URL_TA") != null? e.getAs("URL_TA").toString() : "/";
		 					
		 					instance.writeToDB(new HotelModelBean(name, city, ranking, rating, reviewCount, url));			
	 					});
	 
	 log.info("processing Completed!!!");
	 return "OK";
	}
	
	private void writeToDB(HotelModelBean hotelModelBean) throws SolrServerException, IOException {

		String urlString = "http://localhost:8983/solr/spark_hotel";
		SolrClient Solr = new HttpSolrClient.Builder(urlString).build();
		SolrInputDocument doc = new SolrInputDocument();

		doc.addField("name", hotelModelBean.getName());
		doc.addField("city", hotelModelBean.getCity());
		doc.addField("rank", hotelModelBean.getRank());
		doc.addField("rating", hotelModelBean.getRating());
		doc.addField("reviewCount", hotelModelBean.getReviewCount());
		doc.addField("url", hotelModelBean.getUrl());
		Solr.add(doc);

		Solr.commit();
		if (++this.currentCount % 10 == 0)
			log.info("{0} / {1}", this.currentCount , this.maxCount);
	} 

}
