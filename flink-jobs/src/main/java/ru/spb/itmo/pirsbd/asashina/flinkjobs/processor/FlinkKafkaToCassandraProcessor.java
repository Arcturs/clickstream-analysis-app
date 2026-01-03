package ru.spb.itmo.pirsbd.asashina.flinkjobs.processor;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import ru.spb.itmo.pirsbd.asashina.flinkjobs.processor.cassandra.sink.FlinkCassandraDwhSink;
import ru.spb.pirsbd.asashina.common.dto.ClickEvent;
import java.util.Objects;

@Component
public class FlinkKafkaToCassandraProcessor {

    private static final Logger log = LoggerFactory.getLogger(FlinkKafkaToCassandraProcessor.class);

    private final KafkaSource<ClickEvent> kafkaSource;
    private final FlinkCassandraDwhSink cassandraSink;

    public FlinkKafkaToCassandraProcessor(KafkaSource<ClickEvent> kafkaSource, FlinkCassandraDwhSink cassandraSink) {
        this.kafkaSource = kafkaSource;
        this.cassandraSink = cassandraSink;
    }

    public void process() throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        var textStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .filter(Objects::nonNull)
                .setParallelism(3)
                .keyBy(event -> String.format("%d_%s", event.getUserId(), event.getId()));
        var sink = cassandraSink.clickEventSink(textStream);
        env.execute();
    }

}
