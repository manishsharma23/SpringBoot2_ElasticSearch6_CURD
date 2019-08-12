package com.example.elasticsearch;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/rest/users")
public class ESDocumentController {
	@RequestMapping("/")
	public String SpringBootESExample() {
		return "Welcome to Spring Boot Elastic Search Example";
	}

	@PostMapping("/insertsavedata")
	public String insert() throws Exception {
		@SuppressWarnings("resource")
		Client client = new PreBuiltTransportClient(
				Settings.builder().put("client.transport.sniff", true).put("cluster.name", "elasticsearch").build())
						.addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
		String jsonObject = "{\"age\":10,\"dateOfBirth\":1471466076564," + "\"fullName\":\"Hetansh Sharma\"}";
		IndexResponse response = client.prepareIndex("hetansh", "happy")
				.setSource(jsonObject, XContentType.JSON).get();
		return response.getResult().toString();
	}

	@PostMapping("/mytempled")
	public void savadata() throws Exception {

		String indexName = "bharti";
		String indexType = "school";
		//
		// Create an instance of Transport Client
		//
		/*
		 * TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
		 * .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),
		 * "9300"));
		 */
		@SuppressWarnings("resource")
		Client client = new PreBuiltTransportClient(
				Settings.builder().put("client.transport.sniff", true).put("cluster.name", "elasticsearch").build())
						.addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
		//
		// Create the JSON mapping
		//
		String jsonMapping = "{\n" + "  \"properties\": {\n"
				+ "       \"created_on\":  { \"type\": \"date\", \"format\": \"dd-MM-YYYY\" },\n"
				+ "       \"name\": { \"type\": \"text\" },\n" + "       \"emp_count\": { \"type\": \"integer\" }\n"
				+ "    }\n" + " }";
		//
		// Create an empty index
		//
		client.admin().indices().prepareCreate(indexName).get();
		//
		// Put the mapping
		//
		PutMappingRequest pmr = Requests.putMappingRequest(indexName).type(indexType).source(jsonMapping,
				XContentType.JSON);
		client.admin().indices().putMapping(pmr).actionGet();
		//
		// Close the client
		//
		client.close();
		/* run at kibana like below
		 * GET bharti/_search
         * below is output 
		 * { "took" : 0, "timed_out" : false, "_shards" : { "total" : 5, "successful" :
		 * 5, "skipped" : 0, "failed" : 0 }, "hits" : { "total" : 0, "max_score" : null,
		 * "hits" : [ ] } }
		 */

	}

	@PostMapping("/savebuilkdata")
	public String builksave() throws Exception {

		String[][] data = { { "student1", "15", "15-10-2011" }, { "student2", "112110", "10-08-2001" },
				{ "student3", "45123", "16-03-2006" } };
		String indexName = "bharti";
		String indexType = "school";
		Map<String, Object> source = new HashMap<String, Object>();
		//
		// Create an instance of Transport Client
		//
		// TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
		// .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),
		// "9300"));

		@SuppressWarnings("resource")
		Client client = new PreBuiltTransportClient(
				Settings.builder().put("client.transport.sniff", true).put("cluster.name", "elasticsearch").build())
						.addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));

		//
		// Prepare Bulk Request
		//
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		for (String[] entry : data) {
			source.put("name", entry[0]);
			source.put("emp_count", entry[1]);
			source.put("created_on", entry[2]);
			bulkRequest.add(client.prepareIndex(indexName, indexType).setSource(source));
		}
		BulkResponse bulkResponse = bulkRequest.get();

		client.close();
		/* aftre run cammand :: GET bharti/_search 
		 * output below like
		 * { "took" : 1, "timed_out" : false, "_shards" : { "total" : 5, "successful" :
		 * 5, "skipped" : 0, "failed" : 0 }, "hits" : { "total" : 3, "max_score" : 1.0,
		 * "hits" : [ { "_index" : "bharti", "_type" : "school", "_id" :
		 * "R8sih2wBN9Ne31IGdfm4", "_score" : 1.0, "_source" : { "emp_count" : "112110",
		 * "created_on" : "10-08-2001", "name" : "student2" } }, { "_index" : "bharti",
		 * "_type" : "school", "_id" : "SMsih2wBN9Ne31IGdfm4", "_score" : 1.0, "_source"
		 * : { "emp_count" : "45123", "created_on" : "16-03-2006", "name" : "student3" }
		 * }, { "_index" : "bharti", "_type" : "school", "_id" : "Rssih2wBN9Ne31IGdfm4",
		 * "_score" : 1.0, "_source" : { "emp_count" : "15", "created_on" :
		 * "15-10-2011", "name" : "student1" } } ] } }
		 */
		return bulkResponse.status().toString();

	}

	@DeleteMapping("/deletedata")
	public String deletedocument() {
		try {
			@SuppressWarnings("resource")
			Client client = new PreBuiltTransportClient(
					Settings.builder().put("client.transport.sniff", true).put("cluster.name", "elasticsearch").build())
							.addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
			String indexName = "manishsharmaaa";
			client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
			// We wait for one second to let ES delete the river
			Thread.sleep(1000);
			return "deleted".toString();

		} catch (Exception e) {
			return "Failed to delete documents";
		}
	}

	@RequestMapping("/searchdata")
	public String searchableJSON() throws Exception {
		@SuppressWarnings("resource")
		Client client = new PreBuiltTransportClient(
				Settings.builder().put("client.transport.sniff", true).put("cluster.name", "elasticsearch").build())
						.addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
		String indexName = "bharti";
		SearchResponse response = client.prepareSearch(indexName).setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
				.setFrom(0).setSize(60).setExplain(true).get();
		SearchHit[] srchHits = response.getHits().getHits();
		String[] result = new String[srchHits.length];
		int i = 0;
		for (SearchHit srchHit : srchHits) {
			result[i++] = (String) srchHit.getSourceAsMap().get("name");
		}
		Arrays.stream(result).forEach(e -> System.out.println(e));

		return response.status().toString();
	}

	@RequestMapping("/updatedata")
	public String update() throws Exception {
		@SuppressWarnings("resource")
		Client client = new PreBuiltTransportClient(
				Settings.builder().put("client.transport.sniff", true).put("cluster.name", "elasticsearch").build())
						.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

		UpdateRequest updateRequest = new UpdateRequest("manishsharmaaa", "Sharma", "T7XCgWwBoxTI1Ytz4zRm");
		XContentBuilder builder = XContentFactory.jsonBuilder();
		builder.startObject();
		builder.field("fullName", "bharti sharma");
		builder.endObject();
		updateRequest.doc(builder);
		client.update(updateRequest);
		return "updated".toString();
	}

}