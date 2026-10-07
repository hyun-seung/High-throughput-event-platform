package messaging.result;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.concurrent.ExecutionException;

@Component
public class FollowupHttpDispatchSchedule {
    private final FollowupHttpDispatcher dispatcher;

    public FollowupHttpDispatchSchedule(FollowupHttpDispatcher dispatcher) {
        this.dispatcher = Objects.requireNonNull(dispatcher);
    }

    @Scheduled(initialDelayString = "${messaging.result.followup.initial-delay-ms:5000}",
            fixedDelayString = "${messaging.result.followup.poll-ms:5000}")
    public void poll() throws ExecutionException, InterruptedException {
        dispatcher.poll();
    }
}
