package ru.spb.itmo.pirsbd.asashina.flinkjobs.config;

import com.datastax.driver.core.ConsistencyLevel;
import org.apache.flink.connector.cassandra.source.CassandraSource;
import org.apache.flink.streaming.connectors.cassandra.SimpleMapperOptions;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.spb.itmo.pirsbd.asashina.flinkjobs.model.CassandraClickEvent;
import java.time.LocalDateTime;

@Configuration
public class FlinkCassandraSourceConfig {

    private final FlinkCassandraDwhProperties properties;

    public FlinkCassandraSourceConfig(FlinkCassandraDwhProperties properties) {
        this.properties = properties;
    }

    @Bean
    public CassandraSource<CassandraClickEvent> clickEventSource() {
        return new CassandraSource<>(
                new CustomClusterBuilder(properties),
                CassandraClickEvent.class,
                "select * from clickstream.click_events where created_at >= %s;".formatted(LocalDateTime.now().minusMinutes(1)),
                new SimpleMapperOptions()
                        .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                        .ttl(86400 * 365)
        );
    }

}
