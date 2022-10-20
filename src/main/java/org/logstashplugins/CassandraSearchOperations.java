package org.logstashplugins;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.openjdk.nashorn.api.scripting.NashornScriptEngineFactory;

import javax.script.ScriptEngine;
import javax.script.ScriptException;

public class CassandraSearchOperations implements Closeable
{
	private String cassandraHost;
	
	private int cassandraPort;
	
	private int cassandraFetchLimit;
	
	private Logger log;
		
	private List<Set<String>> inclusiveTagCombinations = new ArrayList<Set<String>>();
	
	private CqlSession session;

	private String dataCenter;
	
	private PreparedStatement preparedLoadStatement;
	
	private PreparedStatement preparedCountStatement;

	private static NashornScriptEngineFactory sef = new org.openjdk.nashorn.api.scripting.NashornScriptEngineFactory();
	
	public CassandraSearchOperations(String aHost, int aPort, int aFetchLimit, String aDataCenter, String aTagsIncl, Logger aLog) throws Exception
	{
		this.cassandraHost = aHost;
		this.cassandraPort = aPort;
		this.cassandraFetchLimit = aFetchLimit;
		this.log = aLog;
		this.dataCenter = aDataCenter;
		this.session = null;
		
		ensureConnected();
		
		refreshTagCombinations(aTagsIncl);
	}

	public void refreshTagCombinations(String aTagsIncl) throws Exception
	{
		ensureConnected();
		
		inclusiveTagCombinations.clear();
		ResultSet rs = session.execute("SELECT DISTINCT tags FROM eventstore.event");
		for (Row row : rs)
		{
			Set<String> tags = row.getSet(0, String.class);
			if (filterTagsToIncludeExpression(aTagsIncl, tags, log))
			{
				inclusiveTagCombinations.add(tags);
			}			
		}
		
		if (inclusiveTagCombinations.size() == 0) // there must be at least one combination included and this must be refreshed regularly
		{
			throw new Exception("No matching tag combinations found passing the specified list.");
		}
	}
	
	public static boolean filterTagsToIncludeExpression(String aTagsIncl, Set<String> eventTags, Logger log)
	{
		String expressionUnified = aTagsIncl;
		
		// first replace with enclosing all tags in '' (as 1)
		for(String tag : eventTags)
		{
			expressionUnified = expressionUnified.replaceAll("'"+tag+"'", "1");
		}
		
		// second, replace bare substrings (as 1)
		for(String tag : eventTags)
		{
			expressionUnified = expressionUnified.replaceAll(tag, "1");
		}
		
		// third, replace all remaining tags (as 0)
		expressionUnified = expressionUnified.replaceAll("['a-zA-Z_+#-]['a-zA-Z_0-9+#-]+", "false");
		expressionUnified = expressionUnified.replaceAll("1", "true");
		expressionUnified = expressionUnified.replaceAll("0", "false");
		
		try
		{
			ScriptEngine nashornEngine = sef.getScriptEngine();
            boolean result = (boolean)nashornEngine.eval(expressionUnified);
            return result;
        }
		catch (ScriptException e)
		{
            log.error("Failed to evaluate expression for tag filtering: '"+aTagsIncl+"' as '"+expressionUnified+"' with error: "+e.getMessage(), e);
        }
		return false;
	}

	private void ensureConnected()
	{
		if (session == null)
		{
			InetSocketAddress cassandraIP = new InetSocketAddress(cassandraHost, cassandraPort);
			session = CqlSession.builder().addContactPoint(cassandraIP)
										  .withRequestTracker(new RequestTracker()
										  {
											@Override
											public void onSuccess(Request request, long latencyNanos, DriverExecutionProfile executionProfile, Node node, String requestLogPrefix) {
												RequestTracker.super.onSuccess(request, latencyNanos, executionProfile, node, requestLogPrefix);
												log.debug("Finished request "+((BoundStatement)request).getPreparedStatement().getQuery()+" in "+latencyNanos/1000000.0+"ms");
											}
											
											@Override
											public void onNodeSuccess(Request request, long latencyNanos, DriverExecutionProfile executionProfile, Node node, String requestLogPrefix)
											{
												RequestTracker.super.onNodeSuccess(request, latencyNanos, executionProfile, node, requestLogPrefix);
												log.debug("Finished request "+((BoundStatement)request).getPreparedStatement().getQuery()+" in "+latencyNanos/1000000.0+"ms");
											}
											
											@Override
											public void onError(Request request, Throwable error, long latencyNanos, DriverExecutionProfile executionProfile, Node node, String requestLogPrefix)
											{
												RequestTracker.super.onError(request, error, latencyNanos, executionProfile, node, requestLogPrefix);
												log.error("Finished request "+((BoundStatement)request).getPreparedStatement().getQuery()+" in "+latencyNanos/1000000.0+"ms with error: "+error.getMessage());
											}

											@Override
											public void onNodeError(Request request, Throwable error, long latencyNanos, DriverExecutionProfile executionProfile, Node node, String requestLogPrefix)
											{
												RequestTracker.super.onNodeError(request, error, latencyNanos, executionProfile, node, requestLogPrefix);
												log.error("Finished request "+((BoundStatement)request).getPreparedStatement().getQuery()+" in "+latencyNanos/1000000.0+"ms with NODE error: "+error.getMessage());
											}

											@Override public void close() throws Exception {}})
					                      .withLocalDatacenter(dataCenter)
					                      .build();
			
			SimpleStatement simpleLoadStatement = SimpleStatement.builder("SELECT uid, created, tags, data, embed FROM eventstore.event WHERE tags IN ? AND created >= ? AND created < ? LIMIT ?")
															     .setConsistencyLevel(DefaultConsistencyLevel.QUORUM)
															     .setTimeout(Duration.ofSeconds(60)) //.setTracing()															     
															     .build();
			
			preparedLoadStatement = session.prepare(simpleLoadStatement);
			
			SimpleStatement simpleCountStatement = SimpleStatement.builder("SELECT uid FROM eventstore.event WHERE tags IN ? AND created >= ? AND created < ? LIMIT ?")//, created, tags
															      .setConsistencyLevel(DefaultConsistencyLevel.QUORUM)
															      .setTimeout(Duration.ofSeconds(60)) //.setTracing()															     
															      .build();

			preparedCountStatement = session.prepare(simpleCountStatement);
		}		
	}
	
	public List<Map<String, Object>> loadCsData(TimeSegment period)
	{
		List<Map<String, Object>> returnValue = new ArrayList<>();
		try
		{
			ensureConnected();
					
			BoundStatement boundStatement = preparedLoadStatement.bind()
														     .set(0, inclusiveTagCombinations, GenericType.listOf(GenericType.setOf(String.class)))
					                                         .setInstant(1, period.getFromTsInstant())
					                                         .setInstant(2, period.getToTsInstant())
					                                         .setInt(3, cassandraFetchLimit);
						
			Set<String> dedupByUid = new HashSet<String>();
			int duplicates = 0;
			ResultSet rs = session.execute(boundStatement);			
			for (Row row : rs)
			{
				Map<String, Object> rowMap = new HashMap<String, Object>();
				String uid = row.getUuid(0).toString();
				if (dedupByUid.contains(uid))
				{
					++duplicates;
				}
				else
				{
					dedupByUid.add(uid);
					rowMap.put("uid", uid);
					rowMap.put("created", row.getInstant(1).toString());
					rowMap.put("tags", new ArrayList<String>(row.getSet(2, String.class)));
					rowMap.put("data", row.getString(3));
					rowMap.put("embed", row.getString(4));
				}

				returnValue.add(rowMap);
			}
			
			if (duplicates > 0)
			{
				log.warn("Found "+duplicates+" duplicates for "+period);
			}
		}
		catch (Exception e)
		{
			log.error("Exception caused by CS Load query, "+e.getMessage(), e);
			
			if (session!=null) session.close();
			session = null;
			throw e;
		}
		
		return returnValue;
	}

	public int countCsData(TimeSegment period)
	{
		try
		{
			ensureConnected();
					
			BoundStatement boundStatement = preparedCountStatement.bind()
														     .set(0, inclusiveTagCombinations, GenericType.listOf(GenericType.setOf(String.class)))
					                                         .setInstant(1, period.getFromTsInstant())
					                                         .setInstant(2, period.getToTsInstant())
					                                         .setInt(3, cassandraFetchLimit);
						
			Set<String> dedupByUid = new HashSet<String>();
			int duplicates = 0;
			ResultSet rs = session.execute(boundStatement);			
			for (Row row : rs)
			{
				String uid = row.getUuid(0).toString();
				if (dedupByUid.contains(uid))
				{
					++duplicates;
				}
				else
				{
					dedupByUid.add(uid);
				}
			}
			
			if (duplicates > 0)
			{
				log.warn("Found "+duplicates+" duplicates for "+period);
			}
			
			return dedupByUid.size();
		}
		catch (Exception e)
		{
			log.error("Exception caused by CS Count query, "+e.getMessage(), e);
			
			if (session!=null) session.close();
			session = null;
			throw e;
		}
	}
	
	@Override
	public void close() throws IOException
	{
		try
		{
			if (session != null)
			{
				session.close();
				session = null;
			}
		}
		catch (Exception e)
		{
			log.error("Error while closing CqlSession: ", e);
		}
	}
}
