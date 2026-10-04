package messaging.common.messages;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Fixed first-send carrier order, skipping every carrier already attempted for the message. */
public final class PrimaryHttpCarrierRouting {
    private static final List<HttpCarrier> ORDER = List.of(HttpCarrier.SKT, HttpCarrier.KT, HttpCarrier.LGU);

    public static Optional<HttpCarrier> nextUntried(Set<HttpCarrier> attempted) {
        Objects.requireNonNull(attempted);
        return ORDER.stream().filter(carrier -> !attempted.contains(carrier)).findFirst();
    }

    private PrimaryHttpCarrierRouting() { }
}
