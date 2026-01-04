package ru.spb.itmo.pirsbd.asashina.flinkjobs.model;

import java.time.LocalDateTime;

public record CleanClickEvent(String id, String type, LocalDateTime createdAt, String sessionId, Integer userId,
                              String url, String deviceType, String elementId, Integer x, Integer y,
                              String eventTitle) {

}
