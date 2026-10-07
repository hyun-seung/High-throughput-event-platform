package messaging.carrier.sender;

import messaging.common.messages.HttpCarrier;
import messaging.common.messages.MessageTopics;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.ConstructorBinding;

import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

@ConfigurationProperties(prefix = "messaging.carrier.sender")
public record CarrierSenderProperties(HttpCarrier carrier, String topic, String groupId,
                                      String baseUrl, String requestPath,
                                      Duration connectTimeout, Duration responseTimeout,
                                      Duration claimTtl, Duration recoveryGrace,
                                      String notOurCarrierCodes, String tpsExceededCodes) {
    @ConstructorBinding
    public CarrierSenderProperties {
        if (carrier == null || topic == null || !MessageTopics.httpSend(carrier).equals(topic)
                || groupId == null || !expectedGroup(carrier).equals(groupId)) {
            throw new IllegalArgumentException("Carrier pod topic and group must match its carrier");
        }
        if (baseUrl == null || baseUrl.isBlank()) throw new IllegalArgumentException("Carrier URL is required");
        URI uri = URI.create(baseUrl);
        if (!("http".equals(uri.getScheme()) || "https".equals(uri.getScheme())) || uri.getHost() == null
                || uri.getUserInfo() != null || uri.getQuery() != null || uri.getFragment() != null) {
            throw new IllegalArgumentException("Invalid carrier URL");
        }
        requestPath = requestPath == null || requestPath.isBlank() ? "/api/v1/messages" : requestPath;
        if (!requestPath.startsWith("/") || requestPath.startsWith("//")) {
            throw new IllegalArgumentException("Carrier request path must be absolute");
        }
        connectTimeout = connectTimeout == null ? Duration.ofSeconds(2) : connectTimeout;
        responseTimeout = responseTimeout == null ? Duration.ofSeconds(5) : responseTimeout;
        claimTtl = claimTtl == null ? Duration.ofSeconds(30) : claimTtl;
        recoveryGrace = recoveryGrace == null ? Duration.ofSeconds(30) : recoveryGrace;
        if (connectTimeout.isNegative() || connectTimeout.isZero()
                || responseTimeout.isNegative() || responseTimeout.isZero()
                || claimTtl.isNegative() || claimTtl.isZero()
                || recoveryGrace.isNegative() || recoveryGrace.isZero()) {
            throw new IllegalArgumentException("Carrier HTTP durations must be positive");
        }
        if (notOurCarrierCodes == null || notOurCarrierCodes.isBlank()
                || tpsExceededCodes == null || tpsExceededCodes.isBlank()) {
            throw new IllegalArgumentException("Carrier mismatch and TPS provider codes must be configured");
        }
    }

    public Set<String> notOurCarrierCodeSet() { return codes(notOurCarrierCodes); }
    public Set<String> tpsExceededCodeSet() { return codes(tpsExceededCodes); }

    private static Set<String> codes(String csv) {
        return Arrays.stream(csv.split(",")).map(String::trim).filter(s -> !s.isEmpty())
                .collect(Collectors.toUnmodifiableSet());
    }

    private static String expectedGroup(HttpCarrier carrier) {
        return "messaging-" + carrier.name().toLowerCase() + "-http-sender";
    }
}
