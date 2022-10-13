# CassandraSync Logstash Java Plugin

This is a [Java plugin](https://www.elastic.co/guide/en/logstash/current/contributing-java-plugin.html) for [Logstash](https://github.com/elastic/logstash). It allows synchronization/loading of events from eventstore in Cassandra database into Elasticsearch.

__TODO__: add more description + config

# Usage

~~~
input
{
  cassandra_sync
  {
    index => "eventstore-iot"
    days => 90
    synclimit => 1000
    elastichost => "192.168.1.12"
    elasticport => 9200
    cassandrahost => "192.168.1.12"
    cassandraport => 9042
    cassandradatacenter => "datacenter1"
    syncperiod => 15
    resyncperiod => 60
    inclusivetags => [ "iot", "apm" ]
  }
}

filter
{
  json
  {
    source => "data"
    target => "data"
  }
  
  json
  {
    source => "embed"
    remove_field => [ "embed" ]
  }  
 
  date
  {
     match => [ "created", "ISO8601" ]
  }
}

output
{
  stdout { codec => rubydebug }

  elasticsearch
  {
    hosts => ["http://192.168.1.12:9200"]
    index => "eventstore-iot"
    document_id => "%{created}-%{uid}"
    #user => "elastic"
    #password => "changeme"
  }
}
~~~


