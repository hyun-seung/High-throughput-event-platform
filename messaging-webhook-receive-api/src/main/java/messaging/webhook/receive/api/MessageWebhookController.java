package messaging.webhook.receive.api;

import messaging.common.messages.HttpCarrier;
import messaging.common.messages.MessageWebhookResult;
import messaging.webhook.receive.service.MessageWebhookReceiveService;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@RestController
public class MessageWebhookController {
    private final MessageWebhookReceiveService service;

    public MessageWebhookController(MessageWebhookReceiveService service) { this.service = service; }

    @PostMapping("/api/v1/message-webhooks/{carrier}")
    public CompletableFuture<ResponseEntity<MessageWebhookReceiveService.Accepted>> receive(
            @AuthenticationPrincipal HttpCarrier carrier, @RequestBody List<MessageWebhookResult> results) {
        return service.accept(carrier, results).thenApply(accepted -> ResponseEntity.accepted().body(accepted));
    }
}
