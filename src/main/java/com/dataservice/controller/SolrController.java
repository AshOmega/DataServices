package com.dataservice.controller;

import java.io.IOException;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/solr")
public class SolrController {
	private static final Logger log = LoggerFactory.getLogger(SolrController.class);
	
	@GetMapping(value = "/")
	public String helloWorldSolr()
	{
		return "Solr application default";
	}
	
	@GetMapping(value = "/solrTestWrite")
	public void solrTestWrite() throws SolrServerException, IOException
	{
		String urlString = "http://localhost:8983/solr/spark_hotel"; 
	      SolrClient Solr = new HttpSolrClient.Builder(urlString).build();   
	      
	      //Preparing the Solr document 
	      SolrInputDocument doc = new SolrInputDocument(); 
	   
	      //Adding fields to the document 
	      doc.addField("id", "003"); 
	      doc.addField("name", "ABC"); 
	      doc.addField("age","34"); 
	      doc.addField("addr","PQR"); 
	         
	      //Adding the document to Solr 
	      Solr.add(doc);   
	      
	      doc.addField("id", "001"); 
	      doc.addField("name", "XYZ"); 
	      doc.addField("age","40"); 
	      doc.addField("addr","XYZ"); 
	         
	      //Adding the document to Solr 
	      Solr.add(doc);   
	         
	      //Saving the changes 
	      Solr.commit(); 
	      log.info("Documents added"); 
	}
	
	@GetMapping(value = "/solrTestUpdate")
	 public void solrTestUpdate() throws SolrServerException, IOException { 

	      String urlString = "http://localhost:8983/solr/my_core"; 
	      SolrClient Solr = new HttpSolrClient.Builder(urlString).build();   
	      
	      SolrInputDocument doc = new SolrInputDocument(); 
	   
	      UpdateRequest updateRequest = new UpdateRequest();  
	      updateRequest.setAction( UpdateRequest.ACTION.COMMIT, false, false);    
	      SolrInputDocument myDocumentInstantlycommited = new SolrInputDocument();  
	      
	      myDocumentInstantlycommited.addField("id", "003"); 
	      myDocumentInstantlycommited.addField("name", "ABC"); 
	      myDocumentInstantlycommited.addField("age","27"); 
	      myDocumentInstantlycommited.addField("addr","PQR"); 
	      
	      updateRequest.add( myDocumentInstantlycommited);  
	      UpdateResponse rsp = updateRequest.process(Solr); 
	      log.info("Documents Updated"); 
	   } 
	
	@GetMapping(value = "/solrTestDelete")
	 public void solrTestDelete() throws SolrServerException, IOException { 
	      String urlString = "http://localhost:8983/solr/my_core"; 
	      SolrClient Solr = new HttpSolrClient.Builder(urlString).build();   
	     
	      SolrInputDocument doc = new SolrInputDocument();   
	      Solr.deleteByQuery("age:40");        
	      Solr.commit(); 
	      log.info("Documents deleted"); 
	   } 
	
	@GetMapping(value = "/solrTestRead")
	 public void solrTestRead() throws SolrServerException, IOException { 
		 String urlString = "http://localhost:8983/solr/my_core"; 
		 SolrClient Solr = new HttpSolrClient.Builder(urlString).build();  
	      
	      SolrQuery query = new SolrQuery();  
	      query.setQuery("*:*");  
	      query.addField("*");  
	      QueryResponse queryResponse = Solr.query(query);  

	      SolrDocumentList docs = queryResponse.getResults();    
	      System.out.println(docs); 
	      System.out.println(docs.get(0)); 

	      Solr.commit();         
	   } 
}
