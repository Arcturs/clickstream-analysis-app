package ru.spb.itmo.pirsbd.asashina.flinkjobs.config;

import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.enumerate.NonSplittingRecursiveEnumerator;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.time.Duration;

@Configuration
public class FlinkMinioSourceConfig {

    private static final Logger log = LoggerFactory.getLogger(FlinkMinioSourceConfig.class);

    private final FlinkMinioSourceProperties properties;

    public FlinkMinioSourceConfig(FlinkMinioSourceProperties properties) {
        this.properties = properties;
    }

    @Bean
    public FileSource<String> fileSource() {
        var inputPath = String.format("s3a://%s/%s", properties.getBucket(), properties.getInputPath());
        var path = new Path(inputPath);

        log.info("Creating MinIO source from path: {}", inputPath);
        return FileSource.forRecordStreamFormat(new TextLineInputFormat(), path)
                .monitorContinuously(Duration.ofSeconds(properties.getWatchInterval()))
                .setFileEnumerator(NonSplittingRecursiveEnumerator::new)
                .build();
    }

}
