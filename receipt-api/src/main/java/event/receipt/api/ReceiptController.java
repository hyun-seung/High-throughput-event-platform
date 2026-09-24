package event.receipt.api;

import event.receipt.service.ReceiptService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;

@RestController
public class ReceiptController {
    private final ReceiptService service;
    public ReceiptController(ReceiptService service) { this.service = service; }

    @PostMapping("/api/v1/receipts/{provider}")
    public CompletableFuture<ResponseEntity<ReceiptService.ReceiptAccepted>> receive(
            @AuthenticationPrincipal String provider, @Valid @RequestBody ReceiptRequest request) {
        return service.accept(provider, request).thenApply(result -> ResponseEntity.accepted().body(result));
    }
}
