package ru.spb.itmo.pirsbd.asashina.flinkjobs.processor;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.cassandra.source.CassandraSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;
import org.springframework.stereotype.Component;
import ru.spb.itmo.pirsbd.asashina.flinkjobs.model.CassandraClickEvent;
import ru.spb.itmo.pirsbd.asashina.flinkjobs.model.CleanClickEvent;
import ru.spb.itmo.pirsbd.asashina.flinkjobs.model.InvalidEvent;
import ru.spb.itmo.pirsbd.asashina.flinkjobs.processor.cassandra.sink.FlinkCassandraCleanDwhSink;
import ru.spb.itmo.pirsbd.asashina.flinkjobs.processor.function.DeduplicationFunction;
import ru.spb.itmo.pirsbd.asashina.flinkjobs.processor.function.EventValidationFunction;

import java.time.Duration;
import java.util.Objects;

@Component
public class FlinkCassandraToCassandraProcessor {

    public static final OutputTag<InvalidEvent> INVALID_EVENTS_TAG = new OutputTag<>("invalid-events") {};

    private final CassandraSource<CassandraClickEvent> clickEventSource;
    private final FlinkCassandraCleanDwhSink cleanDwhSink;

    public FlinkCassandraToCassandraProcessor(
            CassandraSource<CassandraClickEvent> clickEventSource,
            FlinkCassandraCleanDwhSink cleanDwhSink) {

        this.clickEventSource = clickEventSource;
        this.cleanDwhSink = cleanDwhSink;
    }

    public void process() throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        var rawEventsStream = env.fromSource(clickEventSource, WatermarkStrategy.noWatermarks(), "Cassandra source");
        var validatedStream = validateEvents(rawEventsStream);
        var deduplicatedStream = deduplicateEvents(validatedStream);
        var validEventStream = cleanDwhSink.cleanClickEventSink(deduplicatedStream);

        var invalidEventsStream = cleanDwhSink.invalidClickEventSink(validatedStream.getSideOutput(INVALID_EVENTS_TAG));
        env.execute();
    }

    private SingleOutputStreamOperator<CleanClickEvent> validateEvents(DataStream<CassandraClickEvent> rawEvents) {
        return rawEvents.process(new EventValidationFunction()).name("Validate Events");
    }

    private SingleOutputStreamOperator<CleanClickEvent> deduplicateEvents(
            SingleOutputStreamOperator<CleanClickEvent> validatedStream) {

        return validatedStream.keyBy(this::createDeduplicationKey)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(30)))
                .process(new DeduplicationFunction())
                .name("Deduplicate Events")
                .filter(Objects::nonNull)
                .name("Filter Null Events");
    }

    private String createDeduplicationKey(CleanClickEvent event) {
        return String.format("%s_%s_%s_%s_%s",
                event.id(),
                event.userId(),
                event.sessionId(),
                event.type(),
                event.eventTitle()
        );
    }

}
