package event.event.api.event.controller;

import event.common.core.response.ApiResponse;
import event.event.api.security.principal.AuthenticatedUser;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/events")
public class EventController {

    @GetMapping("/test")
    public ResponseEntity<ApiResponse<AuthenticatedUser>> test(@AuthenticationPrincipal AuthenticatedUser user) {
        return ResponseEntity.ok(ApiResponse.ok(user));
    }
}