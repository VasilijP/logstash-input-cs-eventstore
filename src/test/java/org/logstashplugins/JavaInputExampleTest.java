package org.logstashplugins;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

public class JavaInputExampleTest
{
	
	/**
	 * Tag characters: ['a-zA-Z_+#-]['a-zA-Z_0-9+#-]+
	 */
	@Test
	public void testTagFilter()
	{
		
		Set<String> eventTags =  new HashSet<String>();
		eventTags.add("beats_input_codec_plain_applied");
		eventTags.add("camel");
		eventTags.add("test01");
		eventTags.add("filebeat");
		
		assertTrue(CassandraSearchOperations.filterTagsToIncludeExpression("1", eventTags, null));		
		assertTrue(CassandraSearchOperations.filterTagsToIncludeExpression("!0", eventTags, null));
		assertTrue(CassandraSearchOperations.filterTagsToIncludeExpression("camel", eventTags, null));
		assertTrue(CassandraSearchOperations.filterTagsToIncludeExpression("'camel'", eventTags, null));
		assertTrue(CassandraSearchOperations.filterTagsToIncludeExpression("camel || test77", eventTags, null));
		assertTrue(CassandraSearchOperations.filterTagsToIncludeExpression("camel && test01", eventTags, null));
		assertTrue(CassandraSearchOperations.filterTagsToIncludeExpression("beats_input_codec_plain_applied", eventTags, null));
		assertTrue(CassandraSearchOperations.filterTagsToIncludeExpression("beats_input_codec_plain_applied || some_other_underscoredTAG", eventTags, null));
		assertTrue(CassandraSearchOperations.filterTagsToIncludeExpression("'camel' || test77", eventTags, null));
		assertTrue(CassandraSearchOperations.filterTagsToIncludeExpression("camel && 'test01'", eventTags, null));
		assertTrue(CassandraSearchOperations.filterTagsToIncludeExpression("'beats_input_codec_plain_applied'", eventTags, null));
		assertTrue(CassandraSearchOperations.filterTagsToIncludeExpression("'beats_input_codec_plain_applied' || 'some_other_underscoredTAG'", eventTags, null));
		assertTrue(CassandraSearchOperations.filterTagsToIncludeExpression("camel && test01 && !'non_EXISTENT_TAG'", eventTags, null));
		assertTrue(CassandraSearchOperations.filterTagsToIncludeExpression("camel && 'test71' || !'non_EXISTENT_TAG'", eventTags, null));
		
		assertFalse(CassandraSearchOperations.filterTagsToIncludeExpression("0", eventTags, null));
		assertFalse(CassandraSearchOperations.filterTagsToIncludeExpression("!'camel'", eventTags, null));
		assertFalse(CassandraSearchOperations.filterTagsToIncludeExpression("camel && test01 && !filebeat", eventTags, null));
		assertFalse(CassandraSearchOperations.filterTagsToIncludeExpression("camel && test01 && !'filebeat'", eventTags, null));		
	}
	
    /*
    @Test
    public void testJavaInputExample()
    {
        Map<String, Object> configValues = new HashMap<>();
        //configValues.put(JavaInputExample.INDEX_CONFIG.name(), prefix);
        //configValues.put(JavaInputExample.HISTORY_LOAD_DEPTH_DAYS_CONFIG.name(), eventCount);
        configValues.put(CassandraSync.CASSANDRA_INCLUSIVE_TAGS_CONFIG.name(), Arrays.asList("iot", "apm"));
        Configuration config = new ConfigurationImpl(configValues);
        Context context = new Context()
        {
			@Override
			public NamespacedMetric getMetric(Plugin arg0) { return null; }
			@Override
			public Logger getLogger(Plugin arg0) { return null; }
			@Override
			public EventFactory getEventFactory() { return null; }
			@Override
			public DeadLetterQueueWriter getDlqWriter() { return null; }
		};
		
		CassandraSync input = new CassandraSync("test-id", config, context );
        TestConsumer testConsumer = new TestConsumer();
        input.start(testConsumer);
        
        try
        {
			input.awaitStop();
		} 
        catch (InterruptedException e)
        {		
			e.printStackTrace();
		}
    }
    
    private static class TestConsumer implements Consumer<Map<String, Object>>
    {
        @Override
        public void accept(Map<String, Object> event) {}
    }
    */
}
