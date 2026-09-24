package external.api.simulator.delivery.controller;

import external.api.simulator.delivery.dto.ProviderDispatchRequest;
import external.api.simulator.delivery.dto.ProviderDispatchResponse;
import external.api.simulator.delivery.config.SimulatorProperties;
import external.api.simulator.delivery.service.SimulatorLedger;
import external.api.simulator.receipt.SimulatorReceiptSender;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/api/v1/deliveries")
@RequiredArgsConstructor
public class DeliveryProviderController {

    private static final String IDEMPOTENCY_KEY = "Idempotency-Key";

    private final SimulatorLedger ledger;
    private final SimulatorProperties properties;
    private final SimulatorReceiptSender receipts;

    @PostMapping
    public ResponseEntity<ProviderDispatchResponse> receive(
            @RequestHeader(IDEMPOTENCY_KEY) String idempotencyKey,
            @RequestBody ProviderDispatchRequest request
    ) {
        log.debug("Provider delivery received. deliveryId={}, tenantId={}, deliveryType={}",
                request.deliveryId(), request.tenantId(), request.deliveryType());

        var receipt = receipts.plan(false, idempotencyKey, request);
        ProviderDispatchResponse response = ledger.receive(idempotencyKey, request);
        if (response.accepted()) receipts.accepted(receipt);
        // Delay the response after the effect: a client timeout does not undo provider processing.
        try {
            SimulatorReceiptSender.delayResponse(receipt);
            Thread.sleep(properties.responseDelayMillis());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
        }
        HttpStatus status = response.accepted() ? HttpStatus.OK : switch (response.code() == null ? "" : response.code()) {
            case "RETRY_1S", "RETRY_10S" -> HttpStatus.TOO_MANY_REQUESTS;
            case "FALLBACK" -> HttpStatus.SERVICE_UNAVAILABLE;
            case "REJECTED" -> HttpStatus.BAD_REQUEST;
            default -> HttpStatus.INTERNAL_SERVER_ERROR;
        };
        return ResponseEntity.status(status).body(response);
    }
}
