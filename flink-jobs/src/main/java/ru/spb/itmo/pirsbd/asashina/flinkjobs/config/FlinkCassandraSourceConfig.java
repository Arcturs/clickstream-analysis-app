package ru.spb.itmo.pirsbd.asashina.flinkjobs.config;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import org.apache.flink.connector.cassandra.source.CassandraSource;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
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
                new ClusterBuilder() {
                    @Override
                    protected Cluster buildCluster(Cluster.Builder builder) {
                        return new Cluster.Builder()
                                .addContactPoints(properties.getContactPoints().get(0))
                                .withPort(properties.getPort())
                                .build();
                    }
                },
                CassandraClickEvent.class,
                """
                SELECT *
                FROM %s.click_events
                WHERE created_at >= %s
                """.formatted(properties.getKeyspace(), LocalDateTime.now().minusMinutes(1)),
                new SimpleMapperOptions()
                        .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                        .ttl(86400 * 365)
        );
    }

}
