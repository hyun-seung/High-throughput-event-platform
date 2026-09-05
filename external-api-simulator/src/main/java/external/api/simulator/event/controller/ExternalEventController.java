package external.api.simulator.event.controller;

import external.api.simulator.event.dto.ExternalEventRequest;
import external.api.simulator.event.dto.ExternalEventResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;

@Slf4j
@RestController
@RequestMapping("/api/v1/events")
public class ExternalEventController {

    @PostMapping
    public ResponseEntity<ExternalEventResponse> receive(@RequestBody ExternalEventRequest request) {
        log.debug("External event received. eventId={}, userId={}, eventType={}, payload={}",
                request.eventId(), request.userId(), request.eventType(), request.payload());

        ExternalEventResponse response = new ExternalEventResponse(
                request.eventId(),
                true,
                Instant.now()
        );

        return ResponseEntity.ok(response);
    }
}