package messaging.http.sender;

import messaging.common.messages.MessageSubmission;
import messaging.common.messages.MessageTopics;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class HttpRequestConsumer {
    private final HttpSendService service;

    public HttpRequestConsumer(HttpSendService service) { this.service = service; }

    @KafkaListener(topics = MessageTopics.HTTP_REQUESTED, groupId = "messaging-http-sender")
    public void receive(MessageSubmission event) { service.send(event); }
}
