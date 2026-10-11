package messaging.api.messages;

import messaging.common.messages.MessageSubmission;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
@Slf4j
public class MessageReceiveService {
    private final MessageUsageLimiter usage;
    private final MessageDuplicateGuard duplicates;
    private final MessageOriginStore origins;
    private final MessageRequestPublisher publisher;
    private final Clock clock;
    private final long publishTimeoutMs;

    @Autowired
    public MessageReceiveService(MessageUsageLimiter usage, MessageDuplicateGuard duplicates, MessageOriginStore origins,
                               MessageRequestPublisher publisher,
                               @Value("${messaging.admission.publish-timeout-ms:10000}") long publishTimeoutMs) {
        this(usage, duplicates, origins, publisher, Clock.systemUTC(), publishTimeoutMs);
    }

    MessageReceiveService(MessageUsageLimiter usage, MessageDuplicateGuard duplicates,
                        MessageOriginStore origins, MessageRequestPublisher publisher, Clock clock) {
        this(usage, duplicates, origins, publisher, clock, 10000);
    }

    MessageReceiveService(MessageUsageLimiter usage, MessageDuplicateGuard duplicates,
                          MessageOriginStore origins, MessageRequestPublisher publisher, Clock clock,
                          long publishTimeoutMs) {
        this.usage = usage;
        this.duplicates = duplicates;
        this.origins = origins;
        this.publisher = publisher;
        this.clock = clock;
        if (publishTimeoutMs < 1) throw new IllegalArgumentException("publishTimeoutMs must be positive");
        this.publishTimeoutMs = publishTimeoutMs;
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
            if (!existing.clientMsgId().equals(claim.clientMsgId()) || existing.clientId() != clientId
                    || !existing.messageId().equals(request.messageId())
                    || !existing.recipientNumber().equals(request.recipientNumber())
                    || existing.messageCategory() != request.messageCategory()
                    || !existing.payload().equals(request.payload())
                    || !Objects.equals(existing.secondarySendPayload(), request.secondarySendPayload())) {
                throw new MessageAdmissionException(HttpStatus.CONFLICT, "Duplicate key no longer matches ORIGIN");
            }
            return accepted(existing);
        }

        MessageSubmission event = new MessageSubmission(claim.clientMsgId(), clientId, request.messageId(),
                request.recipientNumber(), request.messageCategory(), request.payload(),
                request.secondarySendPayload(), clock.instant());
        try {
            origins.save(event);
        } catch (RuntimeException uncertain) {
            log.warn("Origin admission save unconfirmed: clientMsgId={}, failureType={}, rootCauseType={}",
                    event.clientMsgId(), uncertain.getClass().getName(), rootCauseType(uncertain));
            // PutItem may have committed even when its response was lost; an SDK retry can then
            // fail its condition. Only a strongly consistent read can resolve this execution.
            Optional<MessageSubmission> stored = Optional.empty();
            try {
                stored = lookup(event.clientMsgId());
            } catch (RuntimeException stillUncertain) {
                uncertain.addSuppressed(stillUncertain);
            }
            if (stored.isPresent() && !sameAdmission(stored.get(), event)
                    && uncertain instanceof ConditionalCheckFailedException) {
                duplicates.release(claim);
                throw new MessageAdmissionException(HttpStatus.CONFLICT, "Execution ID already exists", uncertain);
            }
            if (stored.isEmpty() || !sameAdmission(stored.get(), event)) {
                throw new MessageAdmissionException(HttpStatus.SERVICE_UNAVAILABLE,
                        "Origin admission was not confirmed", uncertain);
            }
            event = stored.get();
        }
        return accepted(event);
    }

    private MessageReceiveResponse accepted(MessageSubmission event) {
        // Success requires the broker ack. Keep ORIGIN and the duplicate key on every failure:
        // recovery or a customer retry must reuse this execution even when the ack was lost.
        try {
            publisher.publish(event).get(publishTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw publicationUnconfirmed(event, interrupted);
        } catch (ExecutionException | TimeoutException | RuntimeException unconfirmed) {
            throw publicationUnconfirmed(event, unconfirmed);
        }
        return new MessageReceiveResponse(event.clientMsgId(), "RECEIVED");
    }

    private MessageAdmissionException publicationUnconfirmed(MessageSubmission event, Exception failure) {
        log.warn("Initial Kafka publication unconfirmed: clientMsgId={}", event.clientMsgId());
        return new MessageAdmissionException(HttpStatus.SERVICE_UNAVAILABLE,
                "Kafka publication was not confirmed", failure);
    }

    private Optional<MessageSubmission> lookup(String clientMsgId) { return origins.find(clientMsgId); }

    private static String rootCauseType(Throwable failure) {
        Throwable cause = failure;
        for (int depth = 0; depth < 16 && cause.getCause() != null && cause.getCause() != cause; depth++) {
            cause = cause.getCause();
        }
        return cause.getClass().getName();
    }

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
