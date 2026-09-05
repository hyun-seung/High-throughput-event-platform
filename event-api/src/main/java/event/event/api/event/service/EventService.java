package event.event.api.event.service;

import event.contract.message.EventMessage;
import event.event.api.event.dto.EventRequest;
import event.event.api.event.dto.EventResponse;
import event.event.api.event.kafka.producer.EventProducer;
import event.event.api.security.principal.AuthenticatedUser;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class EventService {

    private final EventProducer eventProducer;

    public EventResponse accept(AuthenticatedUser user, EventRequest request) {
        String eventId = UUID.randomUUID().toString();

        EventMessage message = new EventMessage(
                eventId,
                user.userId(),
                request.eventType(),
                request.payload(),
                Instant.now()
        );

        eventProducer.send(message);

        return new EventResponse(eventId);
    }
}