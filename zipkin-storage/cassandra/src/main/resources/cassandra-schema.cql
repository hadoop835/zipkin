CREATE KEYSPACE IF NOT EXISTS zipkin WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

CREATE TYPE IF NOT EXISTS zipkin.endpoint (
    service_name text,
    ipv4         inet,
    ipv6         blob,
    port         smallint,
);

CREATE TYPE IF NOT EXISTS zipkin.annotation (
    ts bigint,
    v  text,
    ep frozen<endpoint>
);

CREATE TYPE IF NOT EXISTS zipkin.binary_annotation (
    k  text,
    v  blob,
    t  text,
    ep frozen<endpoint>
);

CREATE TABLE IF NOT EXISTS zipkin.traces (
    trace_id            varint,
    ts                  timestamp,
    id                  bigint,
    span_name           text,
    parent_id           bigint,
    duration            bigint,
    annotations         list<frozen<annotation>>,
    binary_annotations  list<frozen<binary_annotation>>,
    all_annotations     text,
    PRIMARY KEY (trace_id, ts, id)
)
    WITH CLUSTERING ORDER BY (ts DESC)
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy'}
    AND default_time_to_live =  604800;


CREATE TABLE zipkin.service_span_name_index (
    service_name  text,      // service name
    span_name     text,      // span name, or blank for queries without span name
    bucket        int,       // time bucket, calculated as ts/interval (in microseconds), for some pre-configured interval like 1 day.
    ts            timeuuid, // start timestamp of the span, truncated to millisecond precision
    trace_id      varint,    // trace ID
    duration      bigint,    // span duration, in microseconds
    PRIMARY KEY ((service_name, span_name, bucket), ts)
)
   WITH CLUSTERING ORDER BY (ts DESC)
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy'}
    AND default_time_to_live =  259200;


CREATE MATERIALIZED VIEW IF NOT EXISTS zipkin.service_names
    AS SELECT service_name, span_name, bucket, ts, trace_id FROM service_span_name_index
            WHERE service_name is not null AND span_name is not null AND bucket is not null AND ts is not null
            PRIMARY KEY (service_name, bucket, span_name, ts)
        WITH CLUSTERING ORDER BY (bucket DESC, ts DESC)
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'unchecked_tombstone_compaction': 'true', 'tombstone_threshold': '0.2'}
        AND default_time_to_live =  259200;

CREATE CUSTOM INDEX ON zipkin.traces (all_annotations) USING 'org.apache.cassandra.index.sasi.SASIIndex'
   WITH OPTIONS = {
    'mode': 'CONTAINS',
    'analyzed': 'true',
    'analyzer_class':'org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer',
    'case_sensitive': 'false'
   };

CREATE CUSTOM INDEX ON zipkin.service_span_name_index (duration) USING 'org.apache.cassandra.index.sasi.SASIIndex'
   WITH OPTIONS = {'mode': 'PREFIX'}    ;


CREATE TABLE IF NOT EXISTS zipkin.dependencies (
    day          timestamp,
    dependencies blob,
    PRIMARY KEY (day)
)
    WITH compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'unchecked_tombstone_compaction': 'true', 'tombstone_threshold': '0.2'}
    AND default_time_to_live =  259200;

