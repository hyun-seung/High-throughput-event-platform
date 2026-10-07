package messaging.tcp.sender;

import messaging.common.messages.MessageTopics;
import messaging.common.messages.RedisSendAttemptGuard;
import messaging.common.messages.SecondarySendCommand;
import messaging.common.messages.TcpSendResult;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

/** One provider call per frozen command, then durable MSG_RESULT publication. */
@Component
public class TcpSendService {
    private final TcpAttemptStore attempts;
    private final RedisSendAttemptGuard redis;
    private final TcpProviderClient provider;
    private final KafkaTemplate<String, TcpSendResult> kafka;
    private final TcpSenderProperties properties;
    private final Clock clock;

    public TcpSendService(TcpAttemptStore attempts, RedisSendAttemptGuard redis, TcpProviderClient provider,
                          KafkaTemplate<String, TcpSendResult> kafka, TcpSenderProperties properties, Clock clock) {
        this.attempts = Objects.requireNonNull(attempts);
        this.redis = Objects.requireNonNull(redis);
        this.provider = Objects.requireNonNull(provider);
        this.kafka = Objects.requireNonNull(kafka);
        this.properties = Objects.requireNonNull(properties);
        this.clock = Objects.requireNonNull(clock);
    }

    public void send(SecondarySendCommand command) throws ExecutionException, InterruptedException {
        var state = attempts.reserve(command, clock.instant());
        switch (state) {
            case INELIGIBLE -> { return; }
            case OBSERVED -> publish(command, attempts.observation(command).orElseThrow());
            case SENDING -> {
                var recovered = attempts.recoverStale(command, clock.instant().minus(properties.recoveryGrace()),
                        provider.timeout(command));
                if (recovered.isEmpty()) throw new IllegalStateException("TCP provider call is still in progress");
                publish(command, recovered.get());
            }
            case RESERVED -> {
                if (!redis.claimTcp(command.submission().clientMsgId(), command.attemptId(), properties.claimTtl())) {
                    throw new IllegalStateException("TCP send claim is held by another execution");
                }
                if (!attempts.begin(command, clock.instant())) {
                    throw new IllegalStateException("TCP send reservation changed before invocation");
                }
                var result = provider.send(command);
                publish(command, attempts.record(command, result));
            }
        }
    }

    private void publish(SecondarySendCommand command, TcpSendResult result)
            throws ExecutionException, InterruptedException {
        if (!attempts.publicationPending(command)) return;
        kafka.send(MessageTopics.MSG_RESULT, command.submission().clientMsgId(), result).get();
        attempts.published(command);
    }
}
