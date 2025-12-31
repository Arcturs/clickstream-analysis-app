package ru.spb.itmo.pirsbd.asashina.flinkjobs.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@ConfigurationPropertiesScan
@ConfigurationProperties(prefix = "spring.minio")
public class FlinkMinioSourceProperties {

    private String bucket;
    private String inputPath;
    private Long watchInterval;

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getInputPath() {
        return inputPath;
    }

    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }

    public Long getWatchInterval() {
        return watchInterval;
    }

    public void setWatchInterval(Long watchInterval) {
        this.watchInterval = watchInterval;
    }
}
