package external.api.simulator.delivery.controller;

import external.api.simulator.delivery.dto.ProviderDispatchRequest;
import external.api.simulator.delivery.dto.ProviderDispatchResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
@RestController
@RequestMapping("/api/v1/deliveries")
public class DeliveryProviderController {

    private static final String FORCE_FAIL = "forceFail";
    private static final String IDEMPOTENCY_KEY = "Idempotency-Key";

    private final ConcurrentMap<String, ProviderDispatchResponse> responses = new ConcurrentHashMap<>();

    @PostMapping
    public ResponseEntity<ProviderDispatchResponse> receive(
            @RequestHeader(IDEMPOTENCY_KEY) String idempotencyKey,
            @RequestBody ProviderDispatchRequest request
    ) {
        log.debug("Provider delivery received. deliveryId={}, tenantId={}, deliveryType={}",
                request.deliveryId(), request.tenantId(), request.deliveryType());

        if (Boolean.TRUE.equals(request.payload().get(FORCE_FAIL))) {
            log.warn("Provider delivery forced to fail. deliveryId={}", request.deliveryId());
            return ResponseEntity
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new ProviderDispatchResponse(request.deliveryId(), false, Instant.now()));
        }

        ProviderDispatchResponse response = responses.computeIfAbsent(
                idempotencyKey,
                ignored -> new ProviderDispatchResponse(request.deliveryId(), true, Instant.now())
        );

        return ResponseEntity.ok(response);
    }
}
