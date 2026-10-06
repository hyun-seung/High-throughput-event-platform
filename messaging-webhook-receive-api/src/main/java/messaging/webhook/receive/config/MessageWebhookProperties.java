package messaging.webhook.receive.config;

import messaging.common.messages.HttpCarrier;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("message.webhook")
public record MessageWebhookProperties(String sktSecret, String ktSecret, String lguSecret) {
    public MessageWebhookProperties {
        sktSecret = sktSecret == null ? "" : sktSecret;
        ktSecret = ktSecret == null ? "" : ktSecret;
        lguSecret = lguSecret == null ? "" : lguSecret;
        for (String secret : new String[]{sktSecret, ktSecret, lguSecret}) {
            if (!secret.isEmpty() && !secret.matches("[!-~]{32,256}")) {
                throw new IllegalArgumentException("Webhook secrets must be empty or 32-256 printable ASCII characters");
            }
        }
        if ((!sktSecret.isEmpty() && (sktSecret.equals(ktSecret) || sktSecret.equals(lguSecret)))
                || (!ktSecret.isEmpty() && ktSecret.equals(lguSecret))) {
            throw new IllegalArgumentException("Carrier webhook secrets must be distinct");
        }
    }

    public String secret(HttpCarrier carrier) {
        return switch (carrier) {
            case SKT -> sktSecret;
            case KT -> ktSecret;
            case LGU -> lguSecret;
        };
    }
}
