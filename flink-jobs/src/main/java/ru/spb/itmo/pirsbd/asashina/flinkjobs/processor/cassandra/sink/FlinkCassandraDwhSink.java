package ru.spb.itmo.pirsbd.asashina.flinkjobs.processor.cassandra.sink;

import com.datastax.driver.core.ConsistencyLevel;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple23;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.SimpleMapperOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import ru.spb.itmo.pirsbd.asashina.flinkjobs.config.FlinkCassandraDwhProperties;
import ru.spb.itmo.pirsbd.asashina.flinkjobs.model.ClickEvent;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
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

    public CassandraSink<Tuple23<Integer, Date, String, String, Date, String,
            String, String, String, String, String,
            String, String, Integer, Integer, String, String,
            String, Integer, Integer, Double, Long, Map<String, String>>>
    clickEventSink(DataStream<ClickEvent> inputStream) throws Exception {

        log.info("Creating Cassandra sink for keyspace: {}", properties.getKeyspace());

        TypeInformation<Tuple23<Integer, Date, String, String, Date, String,
                String, String, String, String, String,
                String, String, Integer, Integer, String, String,
                String, Integer, Integer, Double, Long, Map<String, String>>>
                tupleTypeInfo = new TupleTypeInfo<>(
                Types.INT,       // user_id
                Types.SQL_DATE,  // created_at (Date)
                Types.STRING,    // id
                Types.STRING,    // type
                Types.SQL_DATE,  // received_at (Date)
                Types.STRING,    // session_id
                Types.STRING,    // ip
                Types.STRING,    // url
                Types.STRING,    // referrer
                Types.STRING,    // device_type
                Types.STRING,    // user_agent
                Types.STRING,    // event_title
                Types.STRING,    // element_id
                Types.INT,       // x
                Types.INT,       // y
                Types.STRING,    // element_text
                Types.STRING,    // element_class
                Types.STRING,    // page_title
                Types.INT,       // viewport_width
                Types.INT,       // viewport_height
                Types.DOUBLE,    // scroll_position
                Types.LONG,      // timestamp_offset
                Types.MAP(Types.STRING, Types.STRING) // metadata
        );

        var tupleStream = inputStream
                .map(new ClickEventToTupleMapper())
                .name("Transform to Tuple")
                .returns(tupleTypeInfo);

        var sinkBuilder = CassandraSink.addSink(tupleStream)
                .setHost(properties.getContactPoints().get(0), properties.getPort())
                .setQuery(INSERT_QUERY)
                .setMapperOptions(() -> new SimpleMapperOptions()
                        .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                        .ttl(86400 * 365)
                        .getMapperOptions())
                .setMaxConcurrentRequests(100)
                .enableIgnoreNullFields();

        return sinkBuilder.build();
    }

    public static class ClickEventToTupleMapper implements
            MapFunction<ClickEvent,
                    Tuple23<Integer, Date, String, String, Date, String,
                            String, String, String, String, String,
                            String, String, Integer, Integer, String, String,
                            String, Integer, Integer, Double, Long, Map<String, String>>> {

        @Override
        public Tuple23<Integer, Date, String, String, Date, String,
                String, String, String, String, String,
                String, String, Integer, Integer, String, String,
                String, Integer, Integer, Double, Long, Map<String, String>>
        map(ClickEvent event) {

            Map<String, String> metadata = new HashMap<>();
            if (event.payload() != null && event.payload().metadata() != null) {
                metadata = event.payload().metadata().entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> entry.getValue().toString()
                        ));
            }

            // Преобразуем LocalDateTime в Date для Cassandra
            Date createdAt = event.createdAt() != null
                    ? Date.from(event.createdAt().atZone(ZoneId.systemDefault()).toInstant())
                    : Date.from(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant());

            Date receivedAt = event.receivedAt() != null
                    ? Date.from(event.receivedAt().atZone(ZoneId.systemDefault()).toInstant())
                    : Date.from(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant());

            return new Tuple23<>(
                    event.userId(),
                    createdAt,
                    event.id(),
                    event.type(),
                    receivedAt,
                    event.sessionId(),
                    event.ip(),
                    event.url(),
                    event.referrer(),
                    event.deviceType(),
                    event.userAgent(),
                    event.payload() != null ? event.payload().eventTitle() : null,
                    event.payload() != null ? event.payload().elementId() : null,
                    event.payload() != null ? event.payload().x() : null,
                    event.payload() != null ? event.payload().y() : null,
                    event.payload() != null ? event.payload().elementText() : null,
                    event.payload() != null ? event.payload().elementClass() : null,
                    event.payload() != null ? event.payload().pageTitle() : null,
                    event.payload() != null ? event.payload().viewportWidth() : null,
                    event.payload() != null ? event.payload().viewportHeight() : null,
                    event.payload() != null ? event.payload().scrollPosition() : null,
                    event.payload() != null ? event.payload().timestampOffset() : null,
                    metadata
            );
        }
    }
}