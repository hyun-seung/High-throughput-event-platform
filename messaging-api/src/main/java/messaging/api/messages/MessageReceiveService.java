package messaging.api.messages;

import messaging.common.messages.MessageSubmission;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Service
@Slf4j
public class MessageReceiveService {
    private final MessageUsageLimiter usage;
    private final MessageDuplicateGuard duplicates;
    private final MessageOriginStore origins;
    private final MessageRequestPublisher publisher;
    private final Clock clock;

    @Autowired
    public MessageReceiveService(MessageUsageLimiter usage, MessageDuplicateGuard duplicates, MessageOriginStore origins,
                               MessageRequestPublisher publisher) {
        this(usage, duplicates, origins, publisher, Clock.systemUTC());
    }

    MessageReceiveService(MessageUsageLimiter usage, MessageDuplicateGuard duplicates,
                        MessageOriginStore origins, MessageRequestPublisher publisher, Clock clock) {
        this.usage = usage;
        this.duplicates = duplicates;
        this.origins = origins;
        this.publisher = publisher;
        this.clock = clock;
    }

    public MessageReceiveResponse receive(long clientId, MessageReceiveRequest request) {
        if (request == null || request.messageCategory() == null) {
            throw new MessageAdmissionException(HttpStatus.BAD_REQUEST, "messageCategory is required");
        }
        usage.charge(clientId, request.messageCategory());
        validate(request);
        String proposedClientMsgId = UUID.randomUUID().toString().replace("-", "");
        MessageDuplicateGuard.Claim claim = duplicates.claim(clientId, request, proposedClientMsgId);
        if (!claim.owner()) {
            MessageSubmission existing;
            try {
                existing = lookup(claim.clientMsgId()).orElseThrow(() ->
                        new MessageAdmissionException(HttpStatus.SERVICE_UNAVAILABLE,
                                "Duplicate admission is pending origin confirmation"));
            } catch (MessageAdmissionException known) {
                throw known;
            } catch (RuntimeException unavailable) {
                throw new MessageAdmissionException(HttpStatus.SERVICE_UNAVAILABLE,
                        "Duplicate admission could not be confirmed", unavailable);
            }
            if (existing.clientId() != clientId || !existing.messageId().equals(request.messageId())
                    || !existing.recipientNumber().equals(request.recipientNumber())) {
                throw new MessageAdmissionException(HttpStatus.CONFLICT, "Duplicate key no longer matches ORIGIN");
            }
            return accepted(existing);
        }

        MessageSubmission event = new MessageSubmission(claim.clientMsgId(), clientId, request.messageId(),
                request.recipientNumber(), request.messageCategory(), request.payload(),
                request.secondarySendPayload(), clock.instant());
        try {
            origins.save(event);
        } catch (ConditionalCheckFailedException definitiveFailure) {
            duplicates.release(claim);
            throw new MessageAdmissionException(HttpStatus.CONFLICT, "Execution ID already exists", definitiveFailure);
        } catch (RuntimeException uncertain) {
            // PutItem may have committed even when its response was lost. Never release the Redis key
            // before a strongly consistent read resolves the same execution.
            try {
                Optional<MessageSubmission> stored = lookup(event.clientMsgId());
                if (stored.isPresent() && sameAdmission(stored.get(), event)) {
                    return accepted(stored.get());
                }
            } catch (RuntimeException stillUncertain) {
                uncertain.addSuppressed(stillUncertain);
            }
            throw new MessageAdmissionException(HttpStatus.SERVICE_UNAVAILABLE,
                    "Origin admission was not confirmed", uncertain);
        }
        return accepted(event);
    }

    private MessageReceiveResponse accepted(MessageSubmission event) {
        // ORIGIN is the durable admission proof. Recovery republishes the same execution when
        // Kafka send fails, its ack is lost, or this process exits before the send.
        try {
            publisher.publish(event).whenComplete((ignored, failure) -> {
                if (failure != null) log.warn("Initial Kafka publication unconfirmed: clientMsgId={}", event.clientMsgId());
            });
        } catch (RuntimeException unconfirmed) {
            log.warn("Initial Kafka publication could not start: clientMsgId={}", event.clientMsgId());
        }
        return new MessageReceiveResponse(event.clientMsgId(), "RECEIVED");
    }

    private Optional<MessageSubmission> lookup(String clientMsgId) { return origins.find(clientMsgId); }

    private static boolean sameAdmission(MessageSubmission a, MessageSubmission b) {
        return a.clientMsgId().equals(b.clientMsgId()) && a.clientId() == b.clientId()
                && a.messageId().equals(b.messageId()) && a.recipientNumber().equals(b.recipientNumber())
                && a.messageCategory() == b.messageCategory() && a.payload().equals(b.payload())
                && Objects.equals(a.secondarySendPayload(), b.secondarySendPayload());
    }

    private static void validate(MessageReceiveRequest request) {
        if (request.messageId() == null || request.messageId().isBlank()
                || request.messageId().getBytes(StandardCharsets.UTF_8).length > 40) {
            throw new MessageAdmissionException(HttpStatus.BAD_REQUEST, "messageId is required (up to 40 UTF-8 bytes)");
        }
        if (request.recipientNumber() == null || !request.recipientNumber().matches("010[0-9]{8}")) {
            throw new MessageAdmissionException(HttpStatus.BAD_REQUEST, "recipientNumber must be 010 followed by 8 digits");
        }
        if (request.payload() == null) {
            throw new MessageAdmissionException(HttpStatus.BAD_REQUEST, "payload is required");
        }
        if (request.hasSecondarySendPayload() && request.secondarySendPayload().isEmpty()) {
            throw new MessageAdmissionException(HttpStatus.BAD_REQUEST, "secondarySendPayload must not be empty");
        }
    }
}
