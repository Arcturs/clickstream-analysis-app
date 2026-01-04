package ru.spb.itmo.pirsbd.asashina.flinkjobs.processor.function;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.spb.itmo.pirsbd.asashina.flinkjobs.model.CleanClickEvent;

import java.time.LocalDateTime;
import java.util.*;

public class DeduplicationFunction extends ProcessWindowFunction<CleanClickEvent, CleanClickEvent, String, TimeWindow> {

    private static final Logger log = LoggerFactory.getLogger(DeduplicationFunction.class);

    private transient MapState<String, LocalDateTime> processedEventsState;

    @Override
    public void open(Configuration parameters) {
        var descriptor = new MapStateDescriptor<>(
                "processedEvents",
                TypeInformation.of(String.class),
                TypeInformation.of(LocalDateTime.class)
        );
        processedEventsState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void process(
            String key,
            Context context,
            Iterable<CleanClickEvent> events,
            Collector<CleanClickEvent> out) throws Exception {

        List<CleanClickEvent> eventList = new ArrayList<>();
        events.forEach(eventList::add);
        if (eventList.isEmpty()) {
            return;
        }

        eventList.sort(Comparator.comparing(CleanClickEvent::createdAt));
        Set<String> seenEventIds = new HashSet<>();
        List<CleanClickEvent> cleanedEvents = new ArrayList<>();

        for (var event : eventList) {
            String eventKey = createEventKey(event);
            boolean isDuplicateInState = false;
            try {
                var lastSeen = processedEventsState.get(eventKey);
                if (lastSeen != null) {
                    isDuplicateInState = true;
                    log.debug("Found duplicate event in state: {}", eventKey);
                }
            } catch (Exception e) {
                log.warn("Error accessing state for event: {}", eventKey, e);
            }

            var isDuplicateInWindow = seenEventIds.contains(eventKey);
            if (!isDuplicateInState && !isDuplicateInWindow) {
                cleanedEvents.add(event);
                seenEventIds.add(eventKey);
                processedEventsState.put(eventKey, event.createdAt());
            } else {
                log.debug("Marked as duplicate: {}", eventKey);
            }
        }

        cleanupOldState();
        for (var cleanedEvent : cleanedEvents) {
            out.collect(cleanedEvent);
        }

        log.info(
                "Processed window [{}, {}]: {} events, {} unique",
                new Date(context.window().getStart()),
                new Date(context.window().getEnd()),
                eventList.size(),
                cleanedEvents.size());
    }

    private String createEventKey(CleanClickEvent event) {
        if ("CLICK".equalsIgnoreCase(event.type())) {
            return String.format("%s_%s_%s_%s_%s_%d_%d",
                    event.id(),
                    event.userId(),
                    event.sessionId(),
                    event.type(),
                    event.createdAt() != null ? event.createdAt() : LocalDateTime.now(),
                    event.x() != null ? event.x() : 0,
                    event.y() != null ? event.y() : 0
            );
        }
        return String.format("%s_%s_%s_%s_%s",
                event.id(),
                event.userId(),
                event.sessionId(),
                event.type(),
                event.createdAt() != null ? event.createdAt() : LocalDateTime.now()
        );
    }

    private void cleanupOldState() throws Exception {
        var cutoffTime = LocalDateTime.now().minusSeconds(24 * 60 * 60);
        List<String> keysToRemove = new ArrayList<>();
        var iterator = processedEventsState.iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            if (entry.getValue().isBefore(cutoffTime)) {
                keysToRemove.add(entry.getKey());
            }
        }

        for (var key : keysToRemove) {
            processedEventsState.remove(key);
        }

        if (!keysToRemove.isEmpty()) {
            log.debug("Cleaned up {} old state entries", keysToRemove.size());
        }
    }
}
