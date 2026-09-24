package event.common.tcp;

import java.time.Instant;

/** RECEIVED means application receipt only; final delivery requires a later receipt webhook. */
public record TcpDeliveryResponse(String deliveryId, String attemptId, Boolean accepted, Instant processedAt, String code) { }
