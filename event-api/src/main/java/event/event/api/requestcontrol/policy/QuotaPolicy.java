package event.event.api.requestcontrol.policy;

public record QuotaPolicy(
        boolean enabled,
        long monthlyLimit
) {
}