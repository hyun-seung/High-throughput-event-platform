package event.delivery.dispatch.model;

import java.time.Instant;

public record SecondaryRoute(String attemptId, String provider, Instant deadline) { }
