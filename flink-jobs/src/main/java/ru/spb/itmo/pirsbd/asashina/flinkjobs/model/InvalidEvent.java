package ru.spb.itmo.pirsbd.asashina.flinkjobs.model;

import java.time.LocalDateTime;

public class InvalidEvent {

    private final Integer userId;
    private final String id;
    private final String type;
    private final String sessionId;
    private final LocalDateTime eventTime;
    private final LocalDateTime processedAt;
    private final String validationError;

    public InvalidEvent(
            Integer userId, String id, String type, String sessionId, LocalDateTime eventTime,
            LocalDateTime processedAt, String validationError) {

        this.userId = userId;
        this.id = id;
        this.type = type;
        this.sessionId = sessionId;
        this.eventTime = eventTime;
        this.processedAt = processedAt;
        this.validationError = validationError;
    }
}
