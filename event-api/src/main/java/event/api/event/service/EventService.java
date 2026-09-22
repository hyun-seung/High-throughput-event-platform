package event.api.event.service;

import event.api.event.ingress.EventIngressService;
import event.common.message.EventMessage;
import event.api.event.dto.EventRequest;
import event.api.event.dto.EventResponse;
import event.api.security.principal.AuthenticatedUser;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class EventService {

    private final EventIngressService eventIngressService;

    public EventResponse accept(AuthenticatedUser user, EventRequest request) {
        EventMessage message = new EventMessage(
                UUID.randomUUID().toString(),
                user.userId(),
                request.eventType(),
                request.payload(),
                Instant.now()
        );

        eventIngressService.publish(message);

        return new EventResponse(message.eventId());
    }
}
