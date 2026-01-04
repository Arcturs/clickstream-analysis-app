package ru.spb.itmo.pirsbd.asashina.flinkjobs.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

import java.util.List;

@ConfigurationPropertiesScan
@ConfigurationProperties(prefix = "spring.cassandra.dwh")
public class FlinkCassandraDwhProperties {

    private List<String> contactPoints;
    private int port;
    private String keyspace;
    private String localDatacenter;

    public List<String> getContactPoints() {
        return contactPoints;
    }

    public void setContactPoints(List<String> contactPoints) {
        this.contactPoints = contactPoints;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public String getLocalDatacenter() {
        return localDatacenter;
    }

    public void setLocalDatacenter(String localDatacenter) {
        this.localDatacenter = localDatacenter;
    }

}
