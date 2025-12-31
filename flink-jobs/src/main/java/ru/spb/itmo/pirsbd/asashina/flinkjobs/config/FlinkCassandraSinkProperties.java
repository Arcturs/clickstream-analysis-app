package ru.spb.itmo.pirsbd.asashina.flinkjobs.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

import java.util.List;

@ConfigurationPropertiesScan
@ConfigurationProperties(prefix = "spring.cassandra")
public class FlinkCassandraSinkProperties {

    private List<String> contactPoints;
    private int port;
    private String keyspace;
    private String localDatacenter;
    private String username;
    private String password;

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

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

}
