package event.event.api.event.controller;

import event.common.core.response.ApiResponse;
import event.event.api.security.principal.AuthenticatedUser;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
public class EventController {

    @PostMapping("events")
    public ApiResponse<String> createEvent(@AuthenticationPrincipal AuthenticatedUser user) {
        return new ApiResponse<>(200, "event accepted. userId=" + user.userId());
    }
}