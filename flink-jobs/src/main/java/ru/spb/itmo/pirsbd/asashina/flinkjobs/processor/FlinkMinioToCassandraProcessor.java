package ru.spb.itmo.pirsbd.asashina.flinkjobs.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import ru.spb.pirsbd.asashina.common.dto.ClickEvent;

@Component
public class FlinkMinioToCassandraProcessor {

    private static final Logger log = LoggerFactory.getLogger(FlinkMinioToCassandraProcessor.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final FileSource<String> fileSource;
    private final FlinkCassandraSink cassandraSinkConfig;

    public FlinkMinioToCassandraProcessor(
            FileSource<String> fileSource, 
            FlinkCassandraSink cassandraSinkConfig) {
        this.fileSource = fileSource;
        this.cassandraSinkConfig = cassandraSinkConfig;
    }

    public void process() throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        var textStream = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "MinIO File Source")
                .map(line -> {
                    try {
                        return OBJECT_MAPPER.readValue(line, ClickEvent.class);
                    } catch (Exception e) {
                        log.error("Failed to parse JSON line: {}", line, e);
                        return null;
                    }
                })
                .filter(event -> event != null
                        && event.getUserId() != null
                        && event.getUserId() > 0
                        && ("view".equals(event.getType()) || "click".equals(event.getType())))
                .setParallelism(3)
                .keyBy(event -> String.format("%d_%s", event.getUserId(), event.getId()));
        var sink = cassandraSinkConfig.clickEventSink(textStream);
    }

}
