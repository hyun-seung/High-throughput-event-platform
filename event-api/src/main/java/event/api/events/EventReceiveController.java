package event.api.events;

import event.api.security.principal.AuthenticatedUser;
import event.common.core.response.ApiResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/events")
@RequiredArgsConstructor
public class EventReceiveController {
    private final EventReceiveService service;

    @PostMapping
    public ResponseEntity<ApiResponse<EventReceiveResponse>> receive(
            @AuthenticationPrincipal AuthenticatedUser client,
            @RequestBody EventReceiveRequest request) {
        EventReceiveResponse result = service.receive(client.userId(), request);
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(new ApiResponse<>(HttpStatus.ACCEPTED.value(), result));
    }
}
