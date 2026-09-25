package event.delivery.dispatch.retry;

@FunctionalInterface
public interface RetryPublisher {
    void publish(RetryCommand command);
}
