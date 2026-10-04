package messaging.api.requestcontrol.policy;

public record RequestPolicy(
        Long userId,
        boolean blocked,
        TpsPolicy tpsPolicy,
        QuotaPolicy quotaPolicy
) {
}