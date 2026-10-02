package event.api.events;

import event.common.events.EventSubmission;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;

import java.time.Clock;
import java.util.Optional;
import java.util.UUID;

@Service
@Slf4j
public class EventReceiveService {
    private final EventDuplicateGuard duplicates;
    private final EventOriginStore origins;
    private final EventRequestPublisher publisher;
    private final Clock clock;

    @Autowired
    public EventReceiveService(EventDuplicateGuard duplicates, EventOriginStore origins,
                               EventRequestPublisher publisher) {
        this(duplicates, origins, publisher, Clock.systemUTC());
    }

    EventReceiveService(EventDuplicateGuard duplicates,
                        EventOriginStore origins, EventRequestPublisher publisher, Clock clock) {
        this.duplicates = duplicates;
        this.origins = origins;
        this.publisher = publisher;
        this.clock = clock;
    }

    public EventReceiveResponse receive(long clientId, EventReceiveRequest request) {
        if (request == null || request.eventType() == null) {
            throw new EventAdmissionException(HttpStatus.BAD_REQUEST, "eventType is required");
        }
        validate(request);
        String proposedExecutionId = UUID.randomUUID().toString();
        EventDuplicateGuard.Claim claim = duplicates.claim(clientId, request, proposedExecutionId);
        if (!claim.owner()) {
            EventSubmission existing;
            try {
                existing = lookup(claim.executionId()).orElseThrow(() ->
                        new EventAdmissionException(HttpStatus.SERVICE_UNAVAILABLE,
                                "Duplicate admission is pending origin confirmation"));
            } catch (EventAdmissionException known) {
                throw known;
            } catch (RuntimeException unavailable) {
                throw new EventAdmissionException(HttpStatus.SERVICE_UNAVAILABLE,
                        "Duplicate admission could not be confirmed", unavailable);
            }
            if (existing.clientId() != clientId || !existing.eventId().equals(request.eventId())
                    || !existing.recipientNumber().equals(request.recipientNumber())) {
                throw new EventAdmissionException(HttpStatus.CONFLICT, "Duplicate key no longer matches ORIGIN");
            }
            return accepted(existing);
        }

        EventSubmission event = new EventSubmission(claim.executionId(), clientId, request.eventId(),
                request.recipientNumber(), request.eventType(), request.payload(),
                request.allowFallback(), clock.instant());
        try {
            origins.save(event);
        } catch (ConditionalCheckFailedException definitiveFailure) {
            duplicates.release(claim);
            throw new EventAdmissionException(HttpStatus.CONFLICT, "Execution ID already exists", definitiveFailure);
        } catch (RuntimeException uncertain) {
            // PutItem may have committed even when its response was lost. Never release the Redis key
            // before a strongly consistent read resolves the same execution.
            try {
                Optional<EventSubmission> stored = lookup(event.executionId());
                if (stored.isPresent() && sameAdmission(stored.get(), event)) {
                    return accepted(stored.get());
                }
            } catch (RuntimeException stillUncertain) {
                uncertain.addSuppressed(stillUncertain);
            }
            throw new EventAdmissionException(HttpStatus.SERVICE_UNAVAILABLE,
                    "Origin admission was not confirmed", uncertain);
        }
        return accepted(event);
    }

    private EventReceiveResponse accepted(EventSubmission event) {
        // ORIGIN is the durable admission proof. Recovery republishes the same execution when
        // Kafka send fails, its ack is lost, or this process exits before the send.
        try {
            publisher.publish(event).whenComplete((ignored, failure) -> {
                if (failure != null) log.warn("Initial Kafka publication unconfirmed: executionId={}", event.executionId());
            });
        } catch (RuntimeException unconfirmed) {
            log.warn("Initial Kafka publication could not start: executionId={}", event.executionId());
        }
        return new EventReceiveResponse(event.executionId(), "RECEIVED");
    }

    private Optional<EventSubmission> lookup(String executionId) { return origins.find(executionId); }

    private static boolean sameAdmission(EventSubmission a, EventSubmission b) {
        return a.executionId().equals(b.executionId()) && a.clientId() == b.clientId()
                && a.eventId().equals(b.eventId()) && a.recipientNumber().equals(b.recipientNumber())
                && a.eventType() == b.eventType() && a.payload().equals(b.payload())
                && a.fallbackAllowed() == b.fallbackAllowed();
    }

    private static void validate(EventReceiveRequest request) {
        if (request.eventId() == null || request.eventId().isBlank() || request.eventId().length() > 100) {
            throw new EventAdmissionException(HttpStatus.BAD_REQUEST, "eventId is required (up to 100 characters)");
        }
        if (request.recipientNumber() == null || !request.recipientNumber().matches("010[0-9]{8}")) {
            throw new EventAdmissionException(HttpStatus.BAD_REQUEST, "recipientNumber must be 010 followed by 8 digits");
        }
        if (request.payload() == null) {
            throw new EventAdmissionException(HttpStatus.BAD_REQUEST, "payload is required");
        }
    }
}
