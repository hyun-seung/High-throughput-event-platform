package messaging.common.messages;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Provider wire ID derived from the message's stable sendRequestId and one HTTP invocation. */
public final class ProviderClientMsgIds {
    public record Parsed(String sendRequestId, HttpCarrier carrier, int invocation) { }

    public static String forInvocation(String sendRequestId, HttpCarrier carrier, int invocation) {
        if (sendRequestId == null || sendRequestId.isBlank() || !sendRequestId.equals(sendRequestId.strip())) {
            throw new IllegalArgumentException("Invalid message sendRequestId");
        }
        Objects.requireNonNull(carrier);
        if (invocation < 1) throw new IllegalArgumentException("Invalid HTTP invocation");
        String clientMsgId = sendRequestId + ":" + carrier.name() + ":" + invocation;
        if (clientMsgId.getBytes(StandardCharsets.UTF_8).length > 40) {
            throw new IllegalArgumentException("Provider clientMsgId exceeds 40 UTF-8 bytes");
        }
        return clientMsgId;
    }

    /** The rightmost two segments identify the carrier and invocation; the prefix stays searchable by equality. */
    public static Parsed parse(String clientMsgId) {
        if (clientMsgId == null) throw new IllegalArgumentException("Missing provider clientMsgId");
        int invocationSeparator = clientMsgId.lastIndexOf(':');
        int carrierSeparator = clientMsgId.lastIndexOf(':', invocationSeparator - 1);
        if (carrierSeparator < 1 || invocationSeparator <= carrierSeparator + 1) {
            throw new IllegalArgumentException("Invalid provider clientMsgId");
        }
        try {
            String root = clientMsgId.substring(0, carrierSeparator);
            HttpCarrier carrier = HttpCarrier.valueOf(
                    clientMsgId.substring(carrierSeparator + 1, invocationSeparator));
            int invocation = Integer.parseInt(clientMsgId.substring(invocationSeparator + 1));
            if (!forInvocation(root, carrier, invocation).equals(clientMsgId)) {
                throw new IllegalArgumentException("Noncanonical provider clientMsgId");
            }
            return new Parsed(root, carrier, invocation);
        } catch (RuntimeException invalid) {
            throw new IllegalArgumentException("Invalid provider clientMsgId", invalid);
        }
    }

    private ProviderClientMsgIds() { }
}
