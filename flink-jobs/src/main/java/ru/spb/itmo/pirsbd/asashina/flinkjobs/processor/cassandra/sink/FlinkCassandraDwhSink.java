package ru.spb.itmo.pirsbd.asashina.flinkjobs.processor.cassandra.sink;

import com.datastax.driver.core.ConsistencyLevel;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.SimpleMapperOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import ru.spb.itmo.pirsbd.asashina.flinkjobs.config.FlinkCassandraDwhProperties;
import ru.spb.itmo.pirsbd.asashina.flinkjobs.model.CassandraClickEvent;
import ru.spb.pirsbd.asashina.common.dto.ClickEvent;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class FlinkCassandraDwhSink {

    private static final Logger log = LoggerFactory.getLogger(FlinkCassandraDwhSink.class);
    private static final String INSERT_QUERY = """
            INSERT INTO clickstream.click_events (
                user_id, created_at, id, type, received_at, session_id,
                ip, url, referrer, device_type, user_agent,
                event_title, element_id, x, y, element_text, element_class,
                page_title, viewport_width, viewport_height, scroll_position,
                timestamp_offset, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """;

    private final FlinkCassandraDwhProperties properties;

    public FlinkCassandraDwhSink(FlinkCassandraDwhProperties properties) {
        this.properties = properties;
    }

    public CassandraSink<CassandraClickEvent> clickEventSink(DataStream<ClickEvent> inputStream) throws Exception {
        log.info("Creating Cassandra sink for keyspace: {}", properties.getKeyspace());
        var cassandraStream = inputStream
                .map(new ClickEventToCassandraMapper())
                .name("Transform to Cassandra model")
                .returns(TypeInformation.of(CassandraClickEvent.class));
        var sinkBuilder = CassandraSink.addSink(cassandraStream)
                .setHost(properties.getContactPoints().get(0), properties.getPort())
                .setDefaultKeyspace(properties.getKeyspace())
                .setQuery(INSERT_QUERY)
                .setMapperOptions(() -> new SimpleMapperOptions()
                        .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                        .ttl(86400 * 365)
                        .getMapperOptions())
                .setMaxConcurrentRequests(100)
                .enableIgnoreNullFields();
        return sinkBuilder.build();
    }

    public static class ClickEventToCassandraMapper implements MapFunction<ClickEvent, CassandraClickEvent> {

        @Override
        public CassandraClickEvent map(ClickEvent event) {
            Map<String, String> metadata = new HashMap<>();
            if (event.getPayload() != null && event.getPayload().getMetadata() != null) {
                metadata = event.getPayload().getMetadata().entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> entry.getValue().toString()
                        ));
            }

            return new CassandraClickEvent(
                    event.getUserId(),
                    event.getCreatedAt() != null ? event.getCreatedAt() : LocalDateTime.now(),
                    event.getId(),
                    event.getType(),
                    event.getReceivedAt() != null ? event.getReceivedAt() : LocalDateTime.now(),
                    event.getSessionId(),
                    event.getIp(),
                    event.getUrl(),
                    event.getReferrer(),
                    event.getDeviceType(),
                    event.getUserAgent(),
                    event.getPayload() != null ? event.getPayload().getEventTitle() : null,
                    event.getPayload() != null ? event.getPayload().getElementId() : null,
                    event.getPayload() != null ? event.getPayload().getX() : null,
                    event.getPayload() != null ? event.getPayload().getY() : null,
                    event.getPayload() != null ? event.getPayload().getElementText() : null,
                    event.getPayload() != null ? event.getPayload().getElementClass() : null,
                    event.getPayload() != null ? event.getPayload().getPageTitle() : null,
                    event.getPayload() != null ? event.getPayload().getViewportWidth() : null,
                    event.getPayload() != null ? event.getPayload().getViewportHeight() : null,
                    event.getPayload() != null ? event.getPayload().getScrollPosition() : null,
                    event.getPayload() != null ? event.getPayload().getTimestampOffset() : null,
                    metadata
            );
        }
    }

}
