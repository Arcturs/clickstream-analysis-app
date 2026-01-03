package ru.spb.itmo.pirsbd.asashina.flinkjobs.processor.function;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.spb.itmo.pirsbd.asashina.flinkjobs.dictionary.ValidationStatus;
import ru.spb.itmo.pirsbd.asashina.flinkjobs.model.CassandraClickEvent;
import ru.spb.itmo.pirsbd.asashina.flinkjobs.model.CleanClickEvent;
import ru.spb.itmo.pirsbd.asashina.flinkjobs.model.InvalidEvent;
import java.time.LocalDateTime;
import java.util.*;

import static ru.spb.itmo.pirsbd.asashina.flinkjobs.dictionary.ValidationStatus.*;
import static ru.spb.itmo.pirsbd.asashina.flinkjobs.processor.FlinkCassandraToCassandraProcessor.INVALID_EVENTS_TAG;

public class EventValidationFunction extends ProcessFunction<CassandraClickEvent, CleanClickEvent> {

    private static final Logger log = LoggerFactory.getLogger(EventValidationFunction.class);
    private static final String VIEW_EVENT = "VIEW";
    private static final String CLICK_EVENT = "CLICK";
    private static final List<String> VALID_DEVICE_TYPES = List.of("DESKTOP", "MOBILE", "TABLET", "ANDROID", "IOS");

    private transient OutputTag<InvalidEvent> invalidEventsTag;

    @Override
    public void open(Configuration parameters) {
        invalidEventsTag = INVALID_EVENTS_TAG;
    }

    @Override
    public void processElement(CassandraClickEvent rawEvent, Context ctx, Collector<CleanClickEvent> out) throws Exception {
        try {
            var validationResult = validateEvent(rawEvent);
            if (validationResult.isValid()) {
                var validatedEvent = convertToValidatedEvent(rawEvent);
                out.collect(validatedEvent);
            } else {
                var invalidEvent = createInvalidEvent(rawEvent, validationResult);
                ctx.output(invalidEventsTag, invalidEvent);
                if (validationResult.getStatus() == SUSPICIOUS) {
                    var suspiciousEvent = convertToValidatedEvent(rawEvent);
                    out.collect(suspiciousEvent);
                }
            }
        } catch (Exception e) {
            log.error("Error processing event: {}", rawEvent.getId(), e);
            var invalidEvent = new InvalidEvent(
                    rawEvent.getUserId(),
                    rawEvent.getId(),
                    rawEvent.getType(),
                    rawEvent.getSessionId(),
                    rawEvent.getCreatedAt(),
                    LocalDateTime.now(),
                    "Processing error: " + e.getMessage());
            ctx.output(invalidEventsTag, invalidEvent);
        }
    }

    private ValidationResult validateEvent(CassandraClickEvent event) {
        ValidationResult result = new ValidationResult();

        // 1. Проверка обязательных полей
        if (event.getId() == null || event.getId().trim().isEmpty()) {
            result.addError("Event ID is required");
        }

        if (event.getType() == null || event.getType().trim().isEmpty()) {
            result.addError("Event type is required");
        } else if (!isValidEventType(event.getType())) {
            result.addError("Invalid event type: " + event.getType());
        }

        if (event.getUserId() == null) {
            result.addError("User ID is required");
        } else if (event.getUserId() <= 0) {
            result.addError("User ID must be positive: " + event.getUserId());
        }

        if (event.getSessionId() == null || event.getSessionId().trim().isEmpty()) {
            result.addError("Session ID is required");
        }

        if (event.getUrl() == null || event.getUrl().trim().isEmpty()) {
            result.addError("URL is required");
        }

        if (event.getUserAgent() == null || event.getUserAgent().trim().isEmpty()) {
            result.addError("User agent is required");
        }

        // 2. Проверка временных меток
        if (event.getCreatedAt() == null) {
            result.addError("Created at timestamp is required");
        } else {
            if (event.getCreatedAt().isAfter(LocalDateTime.now())) {
                result.addError("Created at timestamp is in the future: " + event.getCreatedAt());
            }
        }

        if (event.getReceivedAt() != null) {
            if (event.getReceivedAt().isAfter(LocalDateTime.now())) {
                result.addError("Received at timestamp is in the future: " + event.getReceivedAt());
            }

            if (event.getCreatedAt() != null && event.getReceivedAt().isBefore(event.getCreatedAt())) {
                result.addError("Received at cannot be before created at");
            }
        }

        // 3. Проверка типа устройства
        if (event.getDeviceType() != null) {
            if (!VALID_DEVICE_TYPES.contains(event.getDeviceType().toUpperCase())) {
                result.addError("Invalid device type: " + event.getDeviceType());
            }
        }

        // 4. Проверка IP адреса
        if (event.getIp() != null && !isValidIpAddress(event.getIp())) {
            result.addError("Invalid IP address: " + event.getIp());
        }

        // 5. Проверка payload для CLICK событий
        if ("CLICK".equalsIgnoreCase(event.getType())) {
            var elementId = event.getElementId();
            if (elementId == null || elementId.trim().isEmpty()) {
                result.addError("Element ID is required for CLICK events");
            }
        }

        // 6. Проверка длины сессии ID
        if (event.getSessionId() != null && event.getSessionId().length() > 255) {
            result.addError("Session ID too long (max 255 chars)");
        }

        // Определяем статус валидации
        if (result.getErrors().isEmpty()) {
            result.setStatus(VALID);
        } else if (result.hasCriticalErrors()) {
            result.setStatus(INVALID);
        } else {
            result.setStatus(SUSPICIOUS);
        }

        return result;
    }

    private boolean isValidEventType(String eventType) {
        return VIEW_EVENT.equalsIgnoreCase(eventType) || CLICK_EVENT.equalsIgnoreCase(eventType);
    }

    private boolean isValidIpAddress(String ip) {
        if (ip == null || ip.trim().isEmpty()) {
            return false;
        }

        var parts = ip.split("\\.");
        if (parts.length != 4) {
            return false;
        }

        for (var part : parts) {
            try {
                var num = Integer.parseInt(part);
                if (num < 0 || num > 255) {
                    return false;
                }
            } catch (NumberFormatException e) {
                return false;
            }
        }

        return true;
    }

    private CleanClickEvent convertToValidatedEvent(CassandraClickEvent rawEvent) {
        return new CleanClickEvent(
                rawEvent.getId() != null ? rawEvent.getId() : UUID.randomUUID().toString(),
                rawEvent.getType(),
                rawEvent.getCreatedAt(),
                rawEvent.getSessionId(),
                rawEvent.getUserId(),
                rawEvent.getUrl(),
                rawEvent.getDeviceType(),
                rawEvent.getElementId(),
                rawEvent.getX(),
                rawEvent.getY(),
                rawEvent.getEventTitle()
        );
    }

    private InvalidEvent createInvalidEvent(CassandraClickEvent rawEvent, ValidationResult validationResult) {
        return new InvalidEvent(
                rawEvent.getUserId(),
                rawEvent.getId(),
                rawEvent.getType(),
                rawEvent.getSessionId(),
                rawEvent.getCreatedAt(),
                LocalDateTime.now(),
                validationResult.getErrorMessage()
        );
    }

    private static class ValidationResult {

        private ValidationStatus status = VALID;
        private List<String> errors = new ArrayList<>();

        public void addError(String message) {
            errors.add(message);
        }

        public boolean isValid() {
            return errors.isEmpty() || status == VALID;
        }

        public boolean hasCriticalErrors() {
            return status == INVALID;
        }

        public List<String> getErrors() {
            return errors;
        }

        public String getErrorMessage() {
            if (errors.isEmpty()) {
                return "No errors";
            }
            StringBuilder sb = new StringBuilder();
            for (var error : errors) {
                sb.append(error).append(";\n");
            }
            return sb.toString();
        }

        public ValidationStatus getStatus() {
            return status;
        }

        public void setStatus(ValidationStatus status) {
            this.status = status;
        }
    }
}
