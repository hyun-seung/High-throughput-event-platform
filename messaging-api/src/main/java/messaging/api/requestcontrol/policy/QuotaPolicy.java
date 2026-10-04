package messaging.api.requestcontrol.policy;

public record QuotaPolicy(
        boolean enabled,
        long monthlyLimit
) {
}