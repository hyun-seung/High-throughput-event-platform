package event.event.api.event.controller;

import event.common.core.response.ApiResponse;
import event.event.api.event.dto.EventRequest;
import event.event.api.event.dto.EventResponse;
import event.event.api.event.service.EventService;
import event.event.api.security.principal.AuthenticatedUser;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/api/v1/events")
@RequiredArgsConstructor
public class EventController {

    private final EventService eventService;

    @PostMapping
    public ResponseEntity<ApiResponse<EventResponse>> createEvent(
            @AuthenticationPrincipal AuthenticatedUser user,
            @Valid @RequestBody EventRequest request
    ) {
        log.debug("Event request received. userId={}, eventType={}, payload={}",
                user.userId(), request.eventType(), request.payload());

        EventResponse result = eventService.accept(user, request);

        return ResponseEntity
                .status(HttpStatus.ACCEPTED)
                .body(new ApiResponse<>(HttpStatus.ACCEPTED.value(), result));
    }
}