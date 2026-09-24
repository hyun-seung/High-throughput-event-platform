package external.api.simulator.receipt;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.net.URI;
import java.util.Set;

@ConfigurationProperties("simulator.receipts")
public record SimulatorReceiptProperties(@DefaultValue("false") boolean enabled,
        @DefaultValue("http://127.0.0.1:8094/api/v1/receipts/mock-provider") URI primaryUrl,
        @DefaultValue("http://127.0.0.1:8094/api/v1/receipts/tcp-provider") URI secondaryUrl,
        @DefaultValue("") String primarySecret, @DefaultValue("") String secondarySecret,
        @DefaultValue("10000") int maxTracked, @DefaultValue("100") int maxPending,
        @DefaultValue("3") int maxAttempts, @DefaultValue("2000") int timeoutMillis,
        @DefaultValue("1000") int retryDelayMillis) {
    public SimulatorReceiptProperties {
        if (maxTracked < 1 || maxTracked > 1000000 || maxPending < 1 || maxPending > 10000
                || maxAttempts < 1 || maxAttempts > 20 || timeoutMillis < 1 || timeoutMillis > 30000
                || retryDelayMillis < 1 || retryDelayMillis > 60000)
            throw new IllegalArgumentException("Invalid simulator receipt limits");
        if (enabled) {
            validateUrl(primaryUrl); validateUrl(secondaryUrl);
            if (!validSecret(primarySecret) || !validSecret(secondarySecret) || primarySecret.equals(secondarySecret))
                throw new IllegalArgumentException("Distinct simulator receipt secrets of 32..256 printable ASCII characters required");
        }
    }

    private static void validateUrl(URI url) {
        if (url == null || !Set.of("http", "https").contains(url.getScheme()) || url.getHost() == null
                || url.getUserInfo() != null || url.getQuery() != null || url.getFragment() != null)
            throw new IllegalArgumentException("Invalid configured simulator receipt URL");
    }
    private static boolean validSecret(String secret) {
        return secret != null && secret.length() >= 32 && secret.length() <= 256
                && secret.chars().allMatch(c -> c >= 33 && c <= 126);
    }

    // Configuration records must not expose credentials through their generated toString().
    @Override public String toString() { return "SimulatorReceiptProperties[enabled=" + enabled + "]"; }
}
