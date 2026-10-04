package messaging.api.messages;

import messaging.common.messages.MessageSubmission;

import java.util.Optional;

public interface MessageOriginStore {
    void save(MessageSubmission event);
    Optional<MessageSubmission> find(String executionId);
}
