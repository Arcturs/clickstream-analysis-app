package ru.spb.itmo.pirsbd.asashina.flinkjobs.config;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.spb.pirsbd.asashina.common.dto.ClickEvent;

@Configuration
public class FlinkKafkaSourceConfig {

    private final FlinkKafkaSourceProperties properties;

    public FlinkKafkaSourceConfig(FlinkKafkaSourceProperties properties) {
        this.properties = properties;
    }

    @Bean
    public KafkaSource<ClickEvent> kafkaFraudDetectionSource() {
        return KafkaSource.<ClickEvent>builder()
                .setBootstrapServers(properties.getBootstrapServers())
                .setTopics(properties.getTopic())
                .setGroupId(properties.getGroupId() + "-0")
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(ClickEvent.class))
                .build();
    }

}
