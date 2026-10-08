package messaging.api.messages;

import messaging.api.security.AuthenticatedUser;
import messaging.common.core.ApiResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@ConditionalOnProperty(prefix = "messaging.admission", name = "enabled", havingValue = "true")
@RequestMapping("/api/v1/messages")
@RequiredArgsConstructor
public class MessageReceiveController {
    private final MessageReceiveService service;

    @PostMapping
    public ResponseEntity<ApiResponse<MessageReceiveResponse>> receive(
            @AuthenticationPrincipal AuthenticatedUser client,
            @RequestBody MessageReceiveRequest request) {
        MessageReceiveResponse result = service.receive(client.userId(), request);
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(new ApiResponse<>(HttpStatus.ACCEPTED.value(), result));
    }
}
