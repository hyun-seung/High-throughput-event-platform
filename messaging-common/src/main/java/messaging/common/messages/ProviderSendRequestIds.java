package messaging.common.messages;

import java.util.Objects;

/** Provider wire ID derived from the message's stable sendRequestId and one HTTP invocation. */
public final class ProviderSendRequestIds {
    public record Parsed(String sendRequestId, HttpCarrier carrier, int invocation) { }

    public static String forInvocation(String sendRequestId, HttpCarrier carrier, int invocation) {
        if (sendRequestId == null || sendRequestId.isBlank() || !sendRequestId.equals(sendRequestId.strip())) {
            throw new IllegalArgumentException("Invalid message sendRequestId");
        }
        Objects.requireNonNull(carrier);
        if (invocation < 1) throw new IllegalArgumentException("Invalid HTTP invocation");
        return sendRequestId + ":" + carrier.name() + ":" + invocation;
    }

    /** The rightmost two segments identify the carrier and invocation; the prefix stays searchable by equality. */
    public static Parsed parse(String providerSendRequestId) {
        if (providerSendRequestId == null) throw new IllegalArgumentException("Missing provider sendRequestId");
        int invocationSeparator = providerSendRequestId.lastIndexOf(':');
        int carrierSeparator = providerSendRequestId.lastIndexOf(':', invocationSeparator - 1);
        if (carrierSeparator < 1 || invocationSeparator <= carrierSeparator + 1) {
            throw new IllegalArgumentException("Invalid provider sendRequestId");
        }
        try {
            String root = providerSendRequestId.substring(0, carrierSeparator);
            HttpCarrier carrier = HttpCarrier.valueOf(
                    providerSendRequestId.substring(carrierSeparator + 1, invocationSeparator));
            int invocation = Integer.parseInt(providerSendRequestId.substring(invocationSeparator + 1));
            if (!forInvocation(root, carrier, invocation).equals(providerSendRequestId)) {
                throw new IllegalArgumentException("Noncanonical provider sendRequestId");
            }
            return new Parsed(root, carrier, invocation);
        } catch (RuntimeException invalid) {
            throw new IllegalArgumentException("Invalid provider sendRequestId", invalid);
        }
    }

    private ProviderSendRequestIds() { }
}
