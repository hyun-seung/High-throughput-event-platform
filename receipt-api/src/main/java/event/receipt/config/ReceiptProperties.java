package event.receipt.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("receipt")
public record ReceiptProperties(String primaryProvider, String secondaryProvider,
                                String primarySecret, String secondarySecret) {
    public ReceiptProperties {
        primaryProvider = primaryProvider == null ? "mock-provider" : primaryProvider;
        secondaryProvider = secondaryProvider == null ? "tcp-provider" : secondaryProvider;
        primarySecret = primarySecret == null ? "" : primarySecret;
        secondarySecret = secondarySecret == null ? "" : secondarySecret;
        if (!primaryProvider.matches("[A-Za-z0-9_-]{1,64}")
                || !secondaryProvider.matches("[A-Za-z0-9_-]{1,64}") || primaryProvider.equals(secondaryProvider)) {
            throw new IllegalArgumentException("Receipt providers must be distinct safe path identifiers");
        }
        for (String secret : new String[]{primarySecret, secondarySecret}) {
            if (!secret.isEmpty() && !secret.matches("[!-~]{32,256}")) {
                throw new IllegalArgumentException("Receipt secrets must be empty (disabled) or 32-256 printable ASCII characters");
            }
        }
        if (!primarySecret.isEmpty() && primarySecret.equals(secondarySecret)) {
            throw new IllegalArgumentException("Receipt providers must use different secrets");
        }
    }

    public String secret(String provider) {
        if (primaryProvider.equals(provider)) return primarySecret;
        if (secondaryProvider.equals(provider)) return secondarySecret;
        return "";
    }

    public int routeOrder(String provider) {
        if (primaryProvider.equals(provider)) return 1;
        if (secondaryProvider.equals(provider)) return 2;
        throw new IllegalArgumentException("Unknown receipt provider");
    }
}
