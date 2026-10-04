package messaging.api.messages;

import messaging.common.messages.MessageSubmission;

import java.util.concurrent.CompletableFuture;

public interface MessageRequestPublisher {
    CompletableFuture<?> publish(MessageSubmission event);
}
