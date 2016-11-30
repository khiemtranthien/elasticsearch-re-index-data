package com.example.es_reindex_tool;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

public class App {
	
    public static void main( String[] args) throws InterruptedException {
    	String[] nodes = new String[] {"10.2.20.13:9301","10.2.20.14:9303","10.2.20.15:9305"};
    	String clusterName ="prod-es-babe-analytics";
    	
    	int concurrency = 4;
		int bufferMb = 2;
		int actionSize = 1000;
		
		String oldIndex = "install_log_test";
		String newIndex = "install_log";
    	
		//Create Client
    	Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build();
		TransportClient client = new TransportClient(settings);
		for (String node : nodes) {
			String[] hostPost = node.split(":");
			client.addTransportAddress(new InetSocketTransportAddress(hostPost[0], Integer.valueOf(hostPost[1])));
		}
		
		//Create bulk processor
		BulkProcessor bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
			@Override
			public void beforeBulk(long executionId, BulkRequest request) {
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
				int size = request.requests().size();
				if (response.hasFailures()) {
					System.out.println(String.format("Found error after executing elastic bulk. execution id : %s, size : %s, message : [%s], action size : %s",
							executionId, size, response.buildFailureMessage(), request.numberOfActions()));
				} else {
					System.out.println(String.format("Successfully executing bulk. id : %s, size : %s, action size : %s", executionId, size,
							request.numberOfActions()));
				}
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
				System.out.println(String.format("Found error after executing elastic bulk. execution id : %s, size : %s, action size : %s",
						executionId, request.requests().size(), request.numberOfActions()));
			}
		}).setBulkActions(actionSize).setBulkSize(new ByteSizeValue(bufferMb, ByteSizeUnit.MB)).setConcurrentRequests(concurrency).build();
		
		//Get all data using scroll request
		SearchResponse scrollResp = client.prepareSearch(oldIndex) // Specify index
			    .setSearchType(SearchType.SCAN)
			    .setScroll(new TimeValue(60000))
			    .setQuery(QueryBuilders.matchAllQuery()) // Match all query
			    .setSize(100).execute().actionGet(); //100 hits per shard will be returned for each scroll
		System.out.println("Start");
		//Put to new index
		while (true) {
		    scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
		    //Break condition: No hits are returned
		    if (scrollResp.getHits().getHits().length == 0) {
		    	System.out.println("Closing the bulk processor");
		    	bulkProcessor.flush();
		        bulkProcessor.close();
		        break; 
		    }
		    
		    int count = 0;
		    // Get results from a scan search and add it to bulk ingest
		    for (SearchHit hit: scrollResp.getHits()) {
		    	count++;
		    	if(count % 1000 == 0) {
		    		System.out.println(count);
		    	}
		    	
		        IndexRequest request = new IndexRequest(newIndex, hit.type(), hit.id());
		        request.source(hit.sourceRef());
		        bulkProcessor.add(request);
		   }
		}
		
		Thread.sleep(10000);
		System.out.println("End");
    }
}
