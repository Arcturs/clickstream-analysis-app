package ru.spb.itmo.pirsbd.asashina.flinkjobs.model;

import java.time.LocalDateTime;

public record ClickEvent(String id, String type, LocalDateTime createdAt, LocalDateTime receivedAt, String sessionId,
                         String ip, Integer userId, String url, String referrer, String deviceType, String userAgent,
                         ClickEventPayload payload) {

}
