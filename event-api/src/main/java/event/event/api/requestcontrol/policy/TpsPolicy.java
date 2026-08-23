package event.event.api.requestcontrol.policy;

public record TpsPolicy(
        boolean enabled,
        long requestsPerSecond,
        long burstCapacity
) {
}