package ru.spb.itmo.pirsbd.asashina.flinkjobs.model;

import java.util.Map;

public record ClickEventPayload(String eventTitle, String elementId, Integer x, Integer y, String elementText,
                                String elementClass, String pageTitle, Integer viewportWidth, Integer viewportHeight,
                                Double scrollPosition, Long timestampOffset, Map<String, Object> metadata) {

}
