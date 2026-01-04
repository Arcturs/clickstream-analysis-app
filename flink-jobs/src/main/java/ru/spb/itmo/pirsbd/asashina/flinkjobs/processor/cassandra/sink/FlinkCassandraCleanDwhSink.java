package ru.spb.itmo.pirsbd.asashina.flinkjobs.processor.cassandra.sink;

import com.datastax.driver.core.ConsistencyLevel;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.SimpleMapperOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import ru.spb.itmo.pirsbd.asashina.flinkjobs.config.FlinkCassandraDwhProperties;
import ru.spb.itmo.pirsbd.asashina.flinkjobs.model.CleanClickEvent;
import ru.spb.itmo.pirsbd.asashina.flinkjobs.model.InvalidEvent;

@Component
public class FlinkCassandraCleanDwhSink {

    private static final Logger log = LoggerFactory.getLogger(FlinkCassandraCleanDwhSink.class);
    private static final String VALID_EVENTS_INSERT_QUERY = """
            INSERT INTO analytics.clean_click_events (
                user_id, created_at, id, type, session_id,
                url, referrer, device_type, event_title, element_id, x, y
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """;
    private static final String INVALID_EVENTS_INSERT_QUERY = """
            INSERT INTO analytics.invalid_events (
                user_id, id, type, session_id, event_time, processed_at, validation_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """;

    private final FlinkCassandraDwhProperties properties;

    public FlinkCassandraCleanDwhSink(FlinkCassandraDwhProperties properties) {
        this.properties = properties;
    }

    public CassandraSink<CleanClickEvent> cleanClickEventSink(DataStream<CleanClickEvent> inputStream) throws Exception {
        log.info("Creating Cassandra sink for keyspace: {}", properties.getKeyspace());
        var sinkBuilder = CassandraSink.addSink(inputStream)
                .setHost(properties.getContactPoints().get(0), properties.getPort())
                .setDefaultKeyspace(properties.getKeyspace())
                .setQuery(VALID_EVENTS_INSERT_QUERY)
                .setMapperOptions(() -> new SimpleMapperOptions()
                        .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                        .ttl(86400 * 365)
                        .getMapperOptions())
                .setMaxConcurrentRequests(100)
                .enableIgnoreNullFields();
        return sinkBuilder.build();
    }

    public CassandraSink<InvalidEvent> invalidClickEventSink(DataStream<InvalidEvent> inputStream) throws Exception {
        var sinkBuilder = CassandraSink.addSink(inputStream)
                .setHost(properties.getContactPoints().get(0), properties.getPort())
                .setDefaultKeyspace(properties.getKeyspace())
                .setQuery(INVALID_EVENTS_INSERT_QUERY)
                .setMapperOptions(() -> new SimpleMapperOptions()
                        .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                        .ttl(86400 * 7)
                        .getMapperOptions())
                .setMaxConcurrentRequests(20)
                .enableIgnoreNullFields();
        return sinkBuilder.build();
    }

}
