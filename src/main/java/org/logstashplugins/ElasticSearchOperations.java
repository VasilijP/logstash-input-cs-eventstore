package org.logstashplugins;

import java.io.Closeable;
import java.io.IOException;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * Implements Elasticsearch operations necessary for synchronization.
 * 
 * @author Peter
 */
public final class ElasticSearchOperations implements Closeable
{
	private String indexPattern;
	RestHighLevelClient elasticClient;
	private Logger log;

	public ElasticSearchOperations(String aHost, int aPort, String aUser, String aPass, String aIndexPattern, Logger aLog)
	{
		this.indexPattern = aIndexPattern;
		this.log = aLog;
		
		//https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/_encrypted_communication.html
		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(aUser, aPass));
		
		// configurable "http"/"https"
		String[] hostSplit = aHost.split("://");
		String host = aHost;
		if (hostSplit.length > 1)
		{
			host = hostSplit[1];
		}
		String method = hostSplit[0].equalsIgnoreCase("https")?"https":"http";
		this.elasticClient = new RestHighLevelClient(RestClient.builder(new HttpHost(host, aPort, method)).setHttpClientConfigCallback(new HttpClientConfigCallback()
		{
			@Override
			public HttpAsyncClientBuilder customizeHttpClient( HttpAsyncClientBuilder httpClientBuilder)
			{
				if (aUser != null && aUser.length() > 0)
				{
					httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
				}				
			    return httpClientBuilder;
			}
	    }));
	}
	
	public int countEsData(TimeSegment period) throws Exception
	{
		CountRequest countRequest = new CountRequest(indexPattern);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); 
		searchSourceBuilder.query(QueryBuilders.rangeQuery("@timestamp").gte(period.getFromTs()).lt(period.getToTs())); 
		countRequest.source(searchSourceBuilder);
	
		try
		{
			return (int)elasticClient.count(countRequest, RequestOptions.DEFAULT).getCount();
		} 
		catch (ElasticsearchStatusException e)
		{
			if (e.status().getStatus() == 404)
			{
				return 0;
			}
			else
			{
				throw e;
			}
		}
	}

	@Override
	public void close() throws IOException
	{
		try
		{
			if (elasticClient != null)
			{
				elasticClient.close();
				elasticClient = null;
			}			
		}
		catch (Exception e)
		{
			log.error("Error while closing RestHighLevelClient: ", e);
		}		
	}

	public long deleteOlder(TimeSegment planBoundary)
	{
		try
		{
			DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(indexPattern);
			deleteByQueryRequest.setQuery(QueryBuilders.rangeQuery("@timestamp").lt(planBoundary.getFromTs()));
			BulkByScrollResponse response = elasticClient.deleteByQuery(deleteByQueryRequest , RequestOptions.DEFAULT);
			return response.getDeleted();
		} 
		catch (ElasticsearchStatusException e)
		{
			if (e.status().getStatus() == 404)
			{
				return 0;
			}
			else
			{
				throw e;
			}
		} 
		catch (IOException e)
		{
			return 0;
		}		
	}

}
