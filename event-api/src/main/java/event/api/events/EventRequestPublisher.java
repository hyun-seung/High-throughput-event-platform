package event.api.events;

import event.common.events.EventSubmission;

import java.util.concurrent.CompletableFuture;

public interface EventRequestPublisher {
    CompletableFuture<?> publish(EventSubmission event);
}
