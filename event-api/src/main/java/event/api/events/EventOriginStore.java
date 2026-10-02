package event.api.events;

import event.common.events.EventSubmission;

import java.util.Optional;

public interface EventOriginStore {
    void save(EventSubmission event);
    Optional<EventSubmission> find(String executionId);
}
