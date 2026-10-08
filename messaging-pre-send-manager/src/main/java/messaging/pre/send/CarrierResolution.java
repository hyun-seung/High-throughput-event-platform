package messaging.pre.send;

import messaging.common.messages.HttpCarrier;

import java.util.Objects;

/** Whether the first carrier came from the phone cache or the agreed SKT fallback. */
public record CarrierResolution(HttpCarrier carrier, boolean mapped) {
    public CarrierResolution {
        Objects.requireNonNull(carrier);
    }
}
