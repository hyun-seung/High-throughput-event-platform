package event.receipt.api;

import event.common.receipt.ReceiptOutcome;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

import java.time.Instant;

public record ReceiptRequest(
        @NotNull @Pattern(regexp = "[A-Za-z0-9._:-]{1,128}") String receiptId,
        @NotNull @Pattern(regexp = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}") String deliveryId,
        @NotNull @Pattern(regexp = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}") String attemptId,
        @NotNull ReceiptOutcome outcome,
        @NotNull @Pattern(regexp = "[A-Z][A-Z0-9_]{0,63}") String code,
        @NotNull Instant occurredAt,
        @jakarta.validation.constraints.Min(1) @jakarta.validation.constraints.Max(4) Integer invocation) {
    @AssertTrue(message = "Receipt outcome and code must describe a final provider result")
    public boolean isConsistent() {
        if (outcome == null || code == null) return true; // Individual constraints report missing fields.
        return outcome == ReceiptOutcome.DELIVERED ? code.equals("DELIVERED")
                : !java.util.Set.of("DELIVERED", "ACCEPTED", "RECEIVED").contains(code);
    }
}
