package org.logstashplugins;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import org.apache.logging.log4j.Logger;

import com.datastax.oss.driver.shaded.guava.common.base.Stopwatch;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Input;
import co.elastic.logstash.api.LogstashPlugin;
import co.elastic.logstash.api.PluginConfigSpec;

// class name must match plugin name
@LogstashPlugin(name = "cassandra_sync")
public class CassandraSync implements Input
{

	// Target index
	public static final PluginConfigSpec<String> INDEX_CONFIG = PluginConfigSpec.stringSetting("index", "eventstore-iot");
	
	// Ensures that events from today up to today-days day are loaded.  
    public static final PluginConfigSpec<Long> HISTORY_LOAD_DEPTH_DAYS_CONFIG = PluginConfigSpec.numSetting("historydays", 365);
    
    // Limits how many events could be synced at once within one query to CS (make sure that CS query result limit is set higher or equal).  
    public static final PluginConfigSpec<Long> EVENT_SYNC_LIMIT_CONFIG = PluginConfigSpec.numSetting("synclimit", 7500);
    
    // How soon is next round of syncing executed after previous round. (this also tells how long new events could sit in CS DB before being discovered)
    public static final PluginConfigSpec<Long> EVENT_SYNC_PERIOD_SECONDS_CONFIG = PluginConfigSpec.numSetting("syncperiod", 10);
	
	public static final PluginConfigSpec<String> ELASTIC_HOST_CONFIG = PluginConfigSpec.stringSetting("elastichost", "http://192.168.1.12");
	
	public static final PluginConfigSpec<Long> ELASTIC_PORT_CONFIG = PluginConfigSpec.numSetting("elasticport", 9200);
	
	public static final PluginConfigSpec<String> ELASTIC_USER_AUTH_USER_CONFIG = PluginConfigSpec.stringSetting("elasticuser", "");
	
	public static final PluginConfigSpec<String> ELASTIC_USER_AUTH_PASS_CONFIG = PluginConfigSpec.stringSetting("elasticpass", "");
	
	public static final PluginConfigSpec<String> CASSANDRA_HOST_CONFIG = PluginConfigSpec.stringSetting("cassandrahost", "192.168.1.12");
	
	public static final PluginConfigSpec<String> CASSANDRA_DATACENTER_CONFIG = PluginConfigSpec.stringSetting("cassandradatacenter", "datacenter1");
	
	public static final PluginConfigSpec<Long> CASSANDRA_PORT_CONFIG = PluginConfigSpec.numSetting("cassandraport", 9042);
	
	public static final PluginConfigSpec<String> CASSANDRA_INCLUSIVE_TAGS_CONFIG = PluginConfigSpec.stringSetting("inclusivetags", "0");

	private String id;	
	private final CountDownLatch done = new CountDownLatch(1);
	private volatile boolean stopped;
	private Logger log;
	
	private Duration syncMillis;
	private int syncLimit;
	private int loadDepthDays;
	private String elasticHost;
	private int elasticPort;
	private String elasticIndex;
	private String cassandraHost;
	private int cassandraPort;
	private String cassandraDatacenter;
	private String inclusiveTags;
	private String elasticUser;
	private String elasticPass;
		
	public CassandraSync(String id, Configuration config, Context context)
	{
		this.id = id;
		this.log = context.getLogger(this);
		this.syncLimit = config.get(EVENT_SYNC_LIMIT_CONFIG).intValue();
		this.syncMillis = Duration.ofMillis(config.get(EVENT_SYNC_PERIOD_SECONDS_CONFIG) * 1000L);
		this.loadDepthDays = config.get(HISTORY_LOAD_DEPTH_DAYS_CONFIG).intValue();
		this.elasticHost = config.get(ELASTIC_HOST_CONFIG);
		this.elasticUser = config.get(ELASTIC_USER_AUTH_USER_CONFIG);
		this.elasticPass = config.get(ELASTIC_USER_AUTH_PASS_CONFIG);
		this.elasticPort = config.get(ELASTIC_PORT_CONFIG).intValue();
		this.elasticIndex = config.get(INDEX_CONFIG);
		this.cassandraHost = config.get(CASSANDRA_HOST_CONFIG);
		this.cassandraPort = config.get(CASSANDRA_PORT_CONFIG).intValue();
		this.cassandraDatacenter = config.get(CASSANDRA_DATACENTER_CONFIG);
		this.inclusiveTags = config.get(CASSANDRA_INCLUSIVE_TAGS_CONFIG);
				
		log.info("Plugin CassandraSync starting.");
	}

	@Override
	public void start(Consumer<Map<String, Object>> consumer)
	{
		try 
		{
			DataSyncPlan plan = new DataSyncPlan(loadDepthDays, syncLimit);
			ElasticSearchOperations elasticOps = null;
			CassandraSearchOperations cassandraOps = null;
			Stopwatch sw = Stopwatch.createStarted();
			int segmentsProcessed = 0;
			
			while (!stopped)
			{
				try
				{
					if (elasticOps == null || cassandraOps == null)
					{
						// init
						log.info("Plugin CassandraSync initializing.");
						elasticOps = new ElasticSearchOperations(elasticHost, elasticPort, elasticUser, elasticPass, elasticIndex, log);
						cassandraOps = new CassandraSearchOperations(cassandraHost, cassandraPort, syncLimit, cassandraDatacenter, inclusiveTags, log);						
						log.info("Plugin CassandraSync initialized, starting sync.");
					}
										
					sw.reset(); sw.start();
					segmentsProcessed = 0;
					cassandraOps.refreshTagCombinations(inclusiveTags);
					TimeSegment planBoundary = plan.ShiftToNow();
					plan.Repartition();
					long deletedCount = elasticOps.deleteOlder(planBoundary);
					if (deletedCount > 0)
					{
						log.info("Deleted "+deletedCount+" events older than "+planBoundary.getFromTs());
					}
					
					for (TimeSegment ts : plan)
					{
						if (ts.getStatus() == TimeSegmentStatus.Invalid)
						{
							List<Map<String, Object>> data = cassandraOps.loadCsData(ts);
							for (Map<String, Object> ev : data)
							{
								consumer.accept(ev);
							}
							log.info("Pushed "+data.size()+" events for TS: "+ts);
							ts.resetStatus();
							++segmentsProcessed;
						}
						  
						if (ts.getStatus() == TimeSegmentStatus.Dirty)
						{
							int countEs = elasticOps.countEsData(ts);
							if (countEs >= syncLimit)
							{
								ts.setCheckResult(countEs, Integer.MAX_VALUE); // it is clear that we need to load some data (and also split segment, no need to query CS now)
							}
							else
							{ 
								int countCs = cassandraOps.countCsData(ts);
								ts.setCheckResult(countEs, countCs); // segment becomes either Ok or invalid (invalid if count does not match)
								if (ts.getStatus() == TimeSegmentStatus.Invalid) //when segment transitions from dirty -> invalid, neighbouring segments become dirty
								{
									List<Map<String, Object>> data = cassandraOps.loadCsData(ts);
									for (Map<String, Object> ev : data)
									{
										consumer.accept(ev);
									}
									log.info("Pushed "+data.size()+" events for TS: "+ts);
									ts.resetStatus();
									ts.getPrevious().resetStatus(); // Flood 'dirty' to next
									ts.getNext().resetStatus(); // and flood 'dirty' to previous until it is verified by setCheckResult without loading any data.
								}
							}
							++segmentsProcessed;
						}
						
						if (sw.elapsed().compareTo(syncMillis) > 0)
						{
							break;
						}
					}
					
					plan.ShiftToNow();
					plan.Repartition();
					
					// Mark some segments as dirty, depending of how much time is left and prioritize more recent periods
					// sleep for the remaining time of sync period (if there was no work to)
					long elapsed = sw.elapsed().toMillis();
					long remainingMillis = Math.max(0,syncMillis.minus(sw.elapsed()).toMillis());
					long targetWorkMillisRemaining = Math.max(0L, remainingMillis - syncMillis.toMillis()/2);
					long segmentsToMarkDirty = Math.min(1+2*segmentsProcessed, targetWorkMillisRemaining / (2*Math.max(1L, elapsed/(1+segmentsProcessed)))); // balanced to not jump from idle to high processing load
					log.info("Processed "+segmentsProcessed+" segments (plan: "+plan+") in "+elapsed+"ms, going to mark up to "+segmentsToMarkDirty+" segments as dirty and sleep for the remaining "+remainingMillis+"ms");					
					try { Thread.sleep(remainingMillis); } catch (InterruptedException ee) {/*ignore*/};
					plan.markDirty(segmentsToMarkDirty);
				}
				catch (InterruptedException e)
				{
					// reinit after wait period on error
					log.info("InterruptedException while sleeping, Plugin CassandraSync will continue after 30s.");
					try { Thread.sleep(30000); } catch (InterruptedException ee) {/*ignore*/};
				}			
				catch (Exception e)
				{
					log.error("Error while running CassandraSync input plugin. Plugin CassandraSync will continue after 30s.", e);
					try { Thread.sleep(30000); } catch (InterruptedException ee) {/*ignore*/};
					
					if (elasticOps != null)
					{
						try {
							elasticOps.close();
						} catch (IOException e1) {
							log.error("Error while deleting elasticOps", e1);
						}
					}
					elasticOps = null;
					
					if (cassandraOps != null)
					{
						try {
						cassandraOps.close();
						} catch (IOException e1) {
							log.error("Error while deleting cassandraOps", e1);
						}
					}
					cassandraOps = null;
				}
			}
			
		}
		finally
		{
			stopped = true;
			done.countDown();
		}
		
		log.info("Plugin CassandraSync stopped.");			
	}

	@Override
	public void stop()
	{
		stopped = true; // set flag to request cooperative stop of input
	}

	@Override
	public void awaitStop() throws InterruptedException
	{
		done.await(); // blocks until input has stopped
	}

	/**
	 * Returns a list of all configuration options for this plugin.
	 */
	@Override
	public Collection<PluginConfigSpec<?>> configSchema()
	{
		return Arrays.asList(INDEX_CONFIG, 
				             HISTORY_LOAD_DEPTH_DAYS_CONFIG,
				             ELASTIC_HOST_CONFIG, 
				             ELASTIC_PORT_CONFIG,
				             ELASTIC_USER_AUTH_USER_CONFIG,
				             ELASTIC_USER_AUTH_PASS_CONFIG,
				             CASSANDRA_HOST_CONFIG, 
				             CASSANDRA_PORT_CONFIG,
				             CASSANDRA_INCLUSIVE_TAGS_CONFIG,
				             CASSANDRA_DATACENTER_CONFIG,
				             EVENT_SYNC_LIMIT_CONFIG,
				             EVENT_SYNC_PERIOD_SECONDS_CONFIG);
	}

	@Override
	public String getId()
	{
		return this.id;
	}
}
