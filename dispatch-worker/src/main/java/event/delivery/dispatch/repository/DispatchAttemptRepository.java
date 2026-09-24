package event.delivery.dispatch.repository;

import event.common.delivery.DeliveryEvent;
import event.common.lifecycle.LifecycleIndex;
import event.common.delivery.DeliveryIds;
import event.delivery.dispatch.model.DispatchAttempt;
import event.delivery.dispatch.model.DispatchClaim;
import event.delivery.dispatch.model.DispatchFailureDecision;
import event.delivery.dispatch.model.SecondaryRoute;
import event.delivery.dispatch.service.DispatchRetryPolicy;
import event.delivery.dispatch.port.DispatchAttemptStore;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

import java.time.Instant;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;

import static event.common.dynamodb.DynamoDbAttributeNames.RETRY_COUNT;
import static event.common.dynamodb.DynamoDbAttributeNames.NEXT_ATTEMPT_AT;
import static event.common.dynamodb.DynamoDbAttributeNames.PRIMARY_DEADLINE;
import static event.common.dynamodb.DynamoDbAttributeNames.DEADLINE_AT;
import static event.common.dynamodb.DynamoDbAttributeNames.ROUTE_ORDER;
import static event.common.dynamodb.DynamoDbAttributeNames.FAILURE_REASON;
import static event.common.dynamodb.DynamoDbAttributeNames.FAILURE_OBSERVED_AT;

import static event.common.dynamodb.DynamoDbAttributeNames.ATTEMPT_ID;
import static event.common.dynamodb.DynamoDbAttributeNames.CREATED_AT;
import static event.common.dynamodb.DynamoDbAttributeNames.DELIVERY_ID;
import static event.common.dynamodb.DynamoDbAttributeNames.EVENT_ID;
import static event.common.dynamodb.DynamoDbAttributeNames.LEASE_UNTIL;
import static event.common.dynamodb.DynamoDbAttributeNames.PK;
import static event.common.dynamodb.DynamoDbAttributeNames.PROVIDER;
import static event.common.dynamodb.DynamoDbAttributeNames.PROVIDER_PROCESSED_AT;
import static event.common.dynamodb.DynamoDbAttributeNames.REVIEW_REASON;
import static event.common.dynamodb.DynamoDbAttributeNames.SK;
import static event.common.dynamodb.DynamoDbAttributeNames.STATUS;
import static event.common.dynamodb.DynamoDbAttributeNames.UPDATED_AT;
import static event.common.dynamodb.DynamoDbAttributeNames.VERSION;
import static event.common.dynamodb.DynamoDbTableNames.DELIVERY_STATE;

@Repository
@RequiredArgsConstructor
public class DispatchAttemptRepository implements DispatchAttemptStore {

    private static final String DELIVERY_PREFIX = "DELIVERY#";
    private static final String ATTEMPT_PREFIX = "ATTEMPT#";
    private static final String PROCESSING = "PROCESSING";
    private static final String ACCEPTED = "ACCEPTED";
    private static final String REVIEW_REQUIRED = "REVIEW_REQUIRED";
    private static final String RETRY_SCHEDULED = "RETRY_SCHEDULED";
    private static final String DECISION_PENDING = "DECISION_PENDING";
    private static final String SECONDARY_ATTEMPT = "secondary_attempt_id";
    private static final String SECONDARY_PROVIDER = "secondary_provider";
    private static final String SECONDARY_DEADLINE = "secondary_deadline";
    private static final Set<String> FALLBACK_REASONS = Set.of("FALLBACK_REQUIRED", "PRIMARY_EXPIRED", "RETRY_EXHAUSTED_NO_RESPONSE");

    private final DynamoDbClient dynamoDbClient;

    @Override
    public DispatchClaim claim(
            DeliveryEvent event,
            String attemptId,
            String provider,
            Instant now,
            Instant leaseUntil,
            Instant primaryDeadline
    ) {
        return claimRoute(event, attemptId, provider, now, leaseUntil, primaryDeadline, 1);
    }

    @Override
    public DispatchClaim claimSecondary(DeliveryEvent event, SecondaryRoute route, Instant now, Instant leaseUntil) {
        return claimRoute(event, route.attemptId(), route.provider(), now, leaseUntil, route.deadline(), 2);
    }

    private DispatchClaim claimRoute(DeliveryEvent event, String attemptId, String provider, Instant now,
                                     Instant leaseUntil, Instant deadline, int routeOrder) {
        Map<String, AttributeValue> key = key(event.deliveryId(), attemptId);
        Map<String, String> names = Map.ofEntries(
                Map.entry("#pk", PK),
                Map.entry("#attemptId", ATTEMPT_ID),
                Map.entry("#deliveryId", DELIVERY_ID),
                Map.entry("#eventId", EVENT_ID),
                Map.entry("#provider", PROVIDER),
                Map.entry("#status", STATUS),
                Map.entry("#leaseUntil", LEASE_UNTIL),
                Map.entry("#createdAt", CREATED_AT),
                Map.entry("#updatedAt", UPDATED_AT),
                Map.entry("#version", VERSION),
                Map.entry("#retryCount", RETRY_COUNT),
                Map.entry("#deadline", DEADLINE_AT),
                Map.entry("#route", ROUTE_ORDER)
        );
        Map<String, AttributeValue> values = Map.ofEntries(
                Map.entry(":attemptId", text(attemptId)),
                Map.entry(":deliveryId", text(event.deliveryId())),
                Map.entry(":eventId", text(event.eventId())),
                Map.entry(":provider", text(provider)),
                Map.entry(":processing", text(PROCESSING)),
                Map.entry(":leaseUntil", number(leaseUntil.toEpochMilli())),
                Map.entry(":now", text(now.toString())),
                Map.entry(":zero", number(0)),
                Map.entry(":one", number(1)),
                Map.entry(":deadline", number(deadline.toEpochMilli())),
                Map.entry(":route", number(routeOrder)),
                Map.entry(":bucket", text(LifecycleIndex.bucket(event.deliveryId())))
        );

        UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName(DELIVERY_STATE)
                .key(key)
                .conditionExpression("attribute_not_exists(#pk)")
                .updateExpression("SET #attemptId = :attemptId, #deliveryId = :deliveryId, "
                        + "#eventId = :eventId, #provider = :provider, #status = :processing, "
                        + "#leaseUntil = :leaseUntil, #createdAt = if_not_exists(#createdAt, :now), "
                        + "#updatedAt = :now, #version = if_not_exists(#version, :zero) + :one, "
                        + "#retryCount = :zero, #deadline = :deadline, #route = :route, "
                        + "lifecycle_bucket = :bucket, lifecycle_due = :deadline")
                .expressionAttributeNames(names)
                .expressionAttributeValues(values)
                .returnValues(ReturnValue.ALL_NEW)
                .build();

        try {
            UpdateItemResponse response = dynamoDbClient.updateItem(request);
            return DispatchClaim.claimed(attempt(response.attributes()));
        } catch (ConditionalCheckFailedException e) {
            return existingClaim(key, now, leaseUntil);
        }
    }

    @Override
    public Optional<SecondaryRoute> prepareSecondary(DeliveryEvent event, String primaryAttemptId, String provider, Duration ttl) {
        if (!event.fallbackAllowed()) return Optional.empty();
        var key = key(event.deliveryId(), primaryAttemptId);
        for (int retry = 0; retry < 2; retry++) {
            var item = dynamoDbClient.getItem(GetItemRequest.builder().tableName(DELIVERY_STATE).key(key)
                    .consistentRead(true).build()).item();
            if (item.isEmpty() || !DECISION_PENDING.equals(item.get(STATUS).s())) {
                throw new IllegalStateException("Primary decision must be durable before secondary dispatch");
            }
            if (!FALLBACK_REASONS.contains(item.get(FAILURE_REASON).s())) return Optional.empty();
            if (item.containsKey(SECONDARY_ATTEMPT)) {
                return Optional.of(new SecondaryRoute(item.get(SECONDARY_ATTEMPT).s(), item.get(SECONDARY_PROVIDER).s(),
                        Instant.ofEpochMilli(Long.parseLong(item.get(SECONDARY_DEADLINE).n()))));
            }
            if (provider.equals(item.get(PROVIDER).s())) throw new IllegalArgumentException("Secondary provider must differ from primary");
            var route = new SecondaryRoute(DeliveryIds.attemptId(event.deliveryId(), provider, 2, 1), provider,
                    Instant.parse(item.get(FAILURE_OBSERVED_AT).s()).plus(ttl));
            try {
                dynamoDbClient.updateItem(UpdateItemRequest.builder().tableName(DELIVERY_STATE).key(key)
                        .conditionExpression("#status = :pending AND #version = :version AND attribute_not_exists(#secondary)")
                        .updateExpression("SET #secondary = :secondary, #provider = :provider, #deadline = :deadline")
                        .expressionAttributeNames(Map.of("#status", STATUS, "#version", VERSION, "#secondary", SECONDARY_ATTEMPT,
                                "#provider", SECONDARY_PROVIDER, "#deadline", SECONDARY_DEADLINE))
                        .expressionAttributeValues(Map.of(":pending", text(DECISION_PENDING), ":version", item.get(VERSION),
                                ":secondary", text(route.attemptId()), ":provider", text(route.provider()),
                                ":deadline", number(route.deadline().toEpochMilli()))).build());
                return Optional.of(route);
            } catch (ConditionalCheckFailedException changed) {
                // Another worker may have bound a route; always use its persisted provider and deadline.
            }
        }
        throw new IllegalStateException("Secondary route changed during handoff; retry resolution");
    }

    @Override
    public void markAccepted(DispatchAttempt attempt, Instant providerProcessedAt, Instant now) {
        UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName(DELIVERY_STATE)
                .key(key(attempt.deliveryId(), attempt.attemptId()))
                .conditionExpression("#status = :processing AND #version = :version")
                .updateExpression("SET #status = :accepted, #providerProcessedAt = :providerProcessedAt, "
                        + "#updatedAt = :now REMOVE #leaseUntil")
                .expressionAttributeNames(Map.of(
                        "#status", STATUS,
                        "#version", VERSION,
                        "#providerProcessedAt", PROVIDER_PROCESSED_AT,
                        "#updatedAt", UPDATED_AT,
                        "#leaseUntil", LEASE_UNTIL
                ))
                .expressionAttributeValues(Map.of(
                        ":processing", text(PROCESSING),
                        ":accepted", text(ACCEPTED),
                        ":version", number(attempt.version()),
                        ":providerProcessedAt", text(providerProcessedAt.toString()),
                        ":now", text(now.toString())
                ))
                .build();

        dynamoDbClient.updateItem(request);
    }

    @Override
    public void recordFailure(DispatchAttempt attempt, DispatchFailureDecision decision) {
        var names = new HashMap<>(Map.of("#status", STATUS, "#version", VERSION, "#reason", FAILURE_REASON,
                "#observed", FAILURE_OBSERVED_AT, "#updated", UPDATED_AT, "#lease", LEASE_UNTIL,
                "#next", NEXT_ATTEMPT_AT));
        var values = new HashMap<>(Map.of(":processing", text(PROCESSING), ":version", number(attempt.version()),
                ":state", text(decision.state().name()), ":reason", text(decision.reason()),
                ":observed", text(decision.observedAt().toString())));
        values.put(":lifecycleDue", number(decision.state() == DispatchFailureDecision.State.DECISION_PENDING ? 0
                : decision.nextAttemptAt() != null ? decision.nextAttemptAt().toEpochMilli() : attempt.deadline().toEpochMilli()));
        String update = "SET lifecycle_due = :lifecycleDue, #status = :state, #reason = :reason, #observed = :observed, #updated = :observed";
        if (decision.state() == DispatchFailureDecision.State.REVIEW_REQUIRED) {
            names.put("#reviewReason", REVIEW_REASON);
            update += ", #reviewReason = :reason";
        }
        if (decision.nextAttemptAt() != null) {
            values.put(":next", number(decision.nextAttemptAt().toEpochMilli()));
            update += ", #next = :next REMOVE #lease";
        } else {
            update += " REMOVE #lease, #next";
        }
        dynamoDbClient.updateItem(UpdateItemRequest.builder().tableName(DELIVERY_STATE)
                .key(key(attempt.deliveryId(), attempt.attemptId()))
                .conditionExpression("#status = :processing AND #version = :version")
                .updateExpression(update).expressionAttributeNames(names).expressionAttributeValues(values).build());
    }

    private DispatchClaim existingClaim(Map<String, AttributeValue> key, Instant now, Instant leaseUntil) {
        // Re-read once if the original worker persists its result while recovery is racing.
        for (int read = 0; read < 2; read++) {
            Map<String, AttributeValue> item = dynamoDbClient.getItem(GetItemRequest.builder()
                            .tableName(DELIVERY_STATE)
                            .key(key)
                            .consistentRead(true)
                            .build())
                    .item();

            AttributeValue status = item.get(STATUS);
            if (status != null && (ACCEPTED.equals(status.s()) || "DELIVERED".equals(status.s()))) {
                return DispatchClaim.alreadyAccepted();
            }
            if (status != null && REVIEW_REQUIRED.equals(status.s())) {
                return DispatchClaim.reviewRequired();
            }
            if (status != null && DECISION_PENDING.equals(status.s())) {
                return DispatchClaim.decisionPending();
            }
            if (status != null && RETRY_SCHEDULED.equals(status.s())) {
                try {
                    return claimScheduled(key, item, now, leaseUntil);
                } catch (ConditionalCheckFailedException changed) {
                    continue;
                }
            }
            if (status == null || !PROCESSING.equals(status.s())) {
                throw new IllegalStateException("Dispatch attempt is missing or has an unsupported state");
            }
            long storedLeaseUntil = Long.parseLong(item.get(LEASE_UNTIL).n());
            if (storedLeaseUntil > now.toEpochMilli()) {
                return DispatchClaim.inProgress();
            }

            long version = Long.parseLong(item.get(VERSION).n());
            try {
                markReviewRequired(key, version, now);
                return DispatchClaim.reviewRequired();
            } catch (ConditionalCheckFailedException changed) {
                // ACCEPTED or REVIEW_REQUIRED may have won; never treat the conflict as permission to send.
            }
        }
        throw new IllegalStateException("Dispatch attempt changed during review handoff; retry state resolution");
    }

    private DispatchClaim claimScheduled(Map<String, AttributeValue> key, Map<String, AttributeValue> item,
                                          Instant now, Instant leaseUntil) {
        String deadlineAttribute = item.containsKey(DEADLINE_AT) ? DEADLINE_AT : PRIMARY_DEADLINE;
        long deadline = Long.parseLong(item.get(deadlineAttribute).n());
        long version = Long.parseLong(item.get(VERSION).n());
        if (now.toEpochMilli() >= deadline) {
            dynamoDbClient.updateItem(UpdateItemRequest.builder().tableName(DELIVERY_STATE).key(key)
                    .conditionExpression("#status = :scheduled AND #version = :version AND #deadline <= :nowMillis")
                    .updateExpression("SET #status = :pending, #reason = :expired, #observed = :at, "
                            + "#updated = :at, #version = #version + :one, lifecycle_due = :nowMillis REMOVE #next")
                    .expressionAttributeNames(Map.of("#status", STATUS, "#version", VERSION, "#deadline", deadlineAttribute,
                            "#reason", FAILURE_REASON, "#observed", FAILURE_OBSERVED_AT, "#updated", UPDATED_AT,
                            "#next", NEXT_ATTEMPT_AT))
                    .expressionAttributeValues(Map.of(":scheduled", text(RETRY_SCHEDULED), ":version", number(version),
                            ":nowMillis", number(now.toEpochMilli()), ":pending", text(DECISION_PENDING),
                            ":expired", text(routeOrder(item) == 2 ? "SECONDARY_EXPIRED" : "PRIMARY_EXPIRED"), ":at", text(Instant.ofEpochMilli(deadline).toString()),
                            ":one", number(1))).build());
            return DispatchClaim.decisionPending();
        }
        if (Long.parseLong(item.get(NEXT_ATTEMPT_AT).n()) > now.toEpochMilli()) {
            return DispatchClaim.retryWait();
        }
        var result = dynamoDbClient.updateItem(UpdateItemRequest.builder().tableName(DELIVERY_STATE).key(key)
                .conditionExpression("#status = :scheduled AND #version = :version AND #next <= :nowMillis "
                        + "AND #deadline > :nowMillis AND #count < :max")
                .updateExpression("SET #status = :processing, #version = #version + :one, #count = #count + :one, "
                        + "#lease = :lease, #updated = :now, lifecycle_due = #deadline REMOVE #next")
                .expressionAttributeNames(Map.of("#status", STATUS, "#version", VERSION, "#next", NEXT_ATTEMPT_AT,
                        "#deadline", deadlineAttribute, "#count", RETRY_COUNT, "#lease", LEASE_UNTIL, "#updated", UPDATED_AT))
                .expressionAttributeValues(Map.of(":scheduled", text(RETRY_SCHEDULED), ":version", number(version),
                        ":nowMillis", number(now.toEpochMilli()), ":max", number(DispatchRetryPolicy.MAX_RETRIES),
                        ":processing", text(PROCESSING), ":one", number(1), ":lease", number(leaseUntil.toEpochMilli()),
                        ":now", text(now.toString())))
                .returnValues(ReturnValue.ALL_NEW).build());
        return DispatchClaim.claimed(attempt(result.attributes()));
    }

    private DispatchAttempt attempt(Map<String, AttributeValue> item) {
        return new DispatchAttempt(item.get(DELIVERY_ID).s(), item.get(ATTEMPT_ID).s(), item.get(PROVIDER).s(),
                Long.parseLong(item.get(VERSION).n()), Integer.parseInt(item.get(RETRY_COUNT).n()),
                Instant.ofEpochMilli(Long.parseLong(item.get(item.containsKey(DEADLINE_AT) ? DEADLINE_AT : PRIMARY_DEADLINE).n())),
                routeOrder(item));
    }

    private int routeOrder(Map<String, AttributeValue> item) {
        return item.containsKey(ROUTE_ORDER) ? Integer.parseInt(item.get(ROUTE_ORDER).n()) : 1;
    }

    private void markReviewRequired(Map<String, AttributeValue> key, long version, Instant now) {
        dynamoDbClient.updateItem(UpdateItemRequest.builder()
                .tableName(DELIVERY_STATE)
                .key(key)
                .conditionExpression("#status = :processing AND #version = :version AND #leaseUntil <= :nowEpochMillis")
                .updateExpression("SET #status = :review, #reviewReason = :reason, #updatedAt = :now, #version = #version + :one")
                .expressionAttributeNames(Map.of(
                        "#status", STATUS, "#version", VERSION, "#leaseUntil", LEASE_UNTIL,
                        "#reviewReason", REVIEW_REASON, "#updatedAt", UPDATED_AT))
                .expressionAttributeValues(Map.of(
                        ":processing", text(PROCESSING), ":review", text(REVIEW_REQUIRED),
                        ":reason", text("LEASE_EXPIRED_WITHOUT_RESULT"),
                        ":version", number(version), ":one", number(1),
                        ":nowEpochMillis", number(now.toEpochMilli()), ":now", text(now.toString())))
                .build());
    }

    private Map<String, AttributeValue> key(String deliveryId, String attemptId) {
        return Map.of(
                PK, text(DELIVERY_PREFIX + deliveryId),
                SK, text(ATTEMPT_PREFIX + attemptId)
        );
    }

    private AttributeValue text(String value) {
        return AttributeValue.fromS(value);
    }

    private AttributeValue number(long value) {
        return AttributeValue.fromN(Long.toString(value));
    }
}
