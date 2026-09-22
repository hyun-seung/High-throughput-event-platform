package event.api.delivery.controller;

import event.api.delivery.dto.DeliveryRequest;
import event.api.delivery.dto.DeliveryResponse;
import event.api.delivery.service.DeliveryService;
import event.api.security.principal.AuthenticatedUser;
import event.common.core.response.ApiResponse;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Validated
@RestController
@RequestMapping("/api/v1/deliveries")
@RequiredArgsConstructor
public class DeliveryController {

    private static final String IDEMPOTENCY_KEY = "Idempotency-Key";

    private final DeliveryService deliveryService;

    @PostMapping
    public CompletableFuture<ResponseEntity<ApiResponse<DeliveryResponse>>> createDelivery(
            @AuthenticationPrincipal AuthenticatedUser user,
            @RequestHeader(IDEMPOTENCY_KEY) @NotBlank @Size(max = 200) String idempotencyKey,
            @Valid @RequestBody DeliveryRequest request
    ) {
        log.debug("Delivery request received. tenantId={}, deliveryType={}", user.userId(), request.deliveryType());

        return deliveryService.accept(user, idempotencyKey, request)
                .thenApply(result -> ResponseEntity
                        .status(HttpStatus.ACCEPTED)
                        .body(new ApiResponse<>(HttpStatus.ACCEPTED.value(), result)));
    }
}
