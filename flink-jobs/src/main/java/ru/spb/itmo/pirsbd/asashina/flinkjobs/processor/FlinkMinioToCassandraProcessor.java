package ru.spb.itmo.pirsbd.asashina.flinkjobs.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import ru.spb.pirsbd.asashina.common.dto.ClickEvent;
import ru.spb.pirsbd.asashina.common.dto.ClickEventPayload;
import java.time.LocalDateTime;
import java.util.*;

import static java.lang.Integer.parseInt;

@Component
public class FlinkMinioToCassandraProcessor {

    private static final Logger log = LoggerFactory.getLogger(FlinkMinioToCassandraProcessor.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final FileSource<String> fileSource;
    private final FlinkCassandraSink cassandraSink;

    public FlinkMinioToCassandraProcessor(FileSource<String> fileSource, FlinkCassandraSink cassandraSink) {
        this.fileSource = fileSource;
        this.cassandraSink = cassandraSink;
    }

    public void process() throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        var textStream = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "MinIO File Source")
                .flatMap(new CsvMapper())
                .filter(event -> event != null
                        && event.getUserId() != null
                        && event.getUserId() > 0
                        && ("view".equals(event.getType()) || "click".equals(event.getType())))
                .setParallelism(3)
                .keyBy(event -> String.format("%d_%s", event.getUserId(), event.getId()));
        var sink = cassandraSink.clickEventSink(textStream);
        env.execute();
    }

    private static class CsvMapper extends RichFlatMapFunction<String, ClickEvent> {

        private List<String> csvHeaders;

        @Override
        public void open(Configuration parameters) throws Exception {
            csvHeaders = Arrays.asList(
                    "id", "type", "created_at", "received_at", "session_id",
                    "ip", "user_id", "url", "referrer", "device_type",
                    "user_agent", "payload_json"
            );
        }

        @Override
        public void flatMap (String line, Collector<ClickEvent> out) throws Exception {
            try {
                if (line.startsWith(csvHeaders.get(0))) {
                    return;
                }

                var values = parseCsvLine(line);
                if (values.size() != csvHeaders.size()) {
                    log.warn("Invalid CSV line format: expected {} columns, got {}",
                            csvHeaders.size(), values.size());
                    return;
                }

                var event = mapCsvToClickEvent(values);
                out.collect(event);
            } catch (Exception e) {
                log.error("Failed to process CSV line: {}", line, e);
            }
        }

        private List<String> parseCsvLine(String line) {
            List<String> result = new ArrayList<>();
            var inQuotes = false;
            var currentField = new StringBuilder();
            for (var c : line.toCharArray()) {
                if (c == '"') {
                    inQuotes = !inQuotes;
                } else if (c == ',' && !inQuotes) {
                    result.add(currentField.toString().trim());
                    currentField.setLength(0);
                } else {
                    currentField.append(c);
                }
            }
            result.add(currentField.toString().trim());
            return result;
        }

        private ClickEvent mapCsvToClickEvent(List<String> values) throws Exception {
            var payloadJson = values.get(11);
            ClickEventPayload payload = null;
            if (payloadJson != null && !payloadJson.trim().isEmpty()) {
                try {
                    var payloadNode = OBJECT_MAPPER.readTree(payloadJson);
                    Map<String, Object> metadata = parseMetadata(payloadNode.get("metadata"));
                    payload = new ClickEventPayload(
                            getTextValue(payloadNode, "event_title"),
                            getTextValue(payloadNode, "element_id"),
                            getIntValue(payloadNode, "x"),
                            getIntValue(payloadNode, "y"),
                            getTextValue(payloadNode, "element_text"),
                            getTextValue(payloadNode, "element_class"),
                            getTextValue(payloadNode, "page_title"),
                            getIntValue(payloadNode, "viewport_width"),
                            getIntValue(payloadNode, "viewport_height"),
                            getDoubleValue(payloadNode, "scroll_position"),
                            getLongValue(payloadNode, "timestamp_offset"),
                            metadata
                    );
                } catch (Exception e) {
                    log.warn("Failed to parse payload JSON: {}", payloadJson, e);
                }
            }

            return new ClickEvent(
                    values.get(0).isEmpty() ? UUID.randomUUID().toString() : values.get(0),
                    values.get(1),
                    values.get(2).isEmpty() ? LocalDateTime.now() : LocalDateTime.parse(values.get(2)),
                    values.get(3).isEmpty() ? LocalDateTime.now() : LocalDateTime.parse(values.get(3)),
                    values.get(4),
                    values.get(5),
                    parseInt(values.get(6)),
                    values.get(7),
                    values.get(8).isEmpty() ? null : values.get(8),
                    values.get(9),
                    values.get(10),
                    payload
            );
        }

        private String getTextValue(JsonNode node, String fieldName) {
            if (node == null || node.get(fieldName) == null || node.get(fieldName).isNull()) {
                return null;
            }
            return node.get(fieldName).asText();
        }

        private Integer getIntValue(JsonNode node, String fieldName) {
            if (node == null || node.get(fieldName) == null || node.get(fieldName).isNull()) {
                return null;
            }
            return node.get(fieldName).asInt();
        }

        private Long getLongValue(JsonNode node, String fieldName) {
            if (node == null || node.get(fieldName) == null || node.get(fieldName).isNull()) {
                return null;
            }
            return node.get(fieldName).asLong();
        }

        private Double getDoubleValue(JsonNode node, String fieldName) {
            if (node == null || node.get(fieldName) == null || node.get(fieldName).isNull()) {
                return null;
            }
            return node.get(fieldName).asDouble();
        }

        private Map<String, Object> parseMetadata(JsonNode metadataNode) {
            if (metadataNode == null || metadataNode.isNull() || metadataNode.isMissingNode()) {
                return null;
            }
            Map<String, Object> metadata = new HashMap<>();
            Iterator<Map.Entry<String, JsonNode>> fields = metadataNode.fields();
            while (fields.hasNext()) {
                var entry = fields.next();
                var value = entry.getValue();
                if (value.isTextual()) {
                    metadata.put(entry.getKey(), value.asText());
                } else if (value.isInt()) {
                    metadata.put(entry.getKey(), value.asInt());
                } else if (value.isLong()) {
                    metadata.put(entry.getKey(), value.asLong());
                } else if (value.isDouble()) {
                    metadata.put(entry.getKey(), value.asDouble());
                } else if (value.isBoolean()) {
                    metadata.put(entry.getKey(), value.asBoolean());
                } else {
                    metadata.put(entry.getKey(), value.toString());
                }
            }
            return metadata;
        }

    }

}
