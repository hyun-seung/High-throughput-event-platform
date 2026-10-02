package event.http.sender;

import event.common.events.EventSubmission;
import event.common.events.EventTopics;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class HttpRequestConsumer {
    private final HttpSendService service;

    public HttpRequestConsumer(HttpSendService service) { this.service = service; }

    @KafkaListener(topics = EventTopics.HTTP_REQUESTED, groupId = "event-http-sender")
    public void receive(EventSubmission event) { service.send(event); }
}
