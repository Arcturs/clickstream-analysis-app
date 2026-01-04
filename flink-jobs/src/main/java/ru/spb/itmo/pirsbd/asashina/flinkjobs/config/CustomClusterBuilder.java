package ru.spb.itmo.pirsbd.asashina.flinkjobs.config;

import com.datastax.driver.core.Cluster;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

public class CustomClusterBuilder extends ClusterBuilder {

    private final FlinkCassandraDwhProperties properties;

    public CustomClusterBuilder(FlinkCassandraDwhProperties properties) {
        this.properties = properties;
    }

    @Override
    protected Cluster buildCluster(Cluster.Builder builder) {
        return new Cluster.Builder()
                .addContactPoints(properties.getContactPoints().get(0))
                .withPort(properties.getPort())
                .build();
    }

}
