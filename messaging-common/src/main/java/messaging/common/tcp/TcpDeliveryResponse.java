package messaging.common.tcp;

import java.time.Instant;

/** Legacy delivery response: RECEIVED is only an application receipt, followed by a result webhook. */
public record TcpDeliveryResponse(String deliveryId, String attemptId, Boolean accepted, Instant processedAt, String code) { }
