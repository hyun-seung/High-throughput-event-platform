package event.processing.worker.event.service;

import event.contract.message.EventMessage;
import event.processing.worker.external.client.ExternalApiClient;
import event.processing.worker.external.dto.ExternalEventResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventProcessingService {

    private final ExternalApiClient externalApiClient;

    public void process(EventMessage message) {
        ExternalEventResponse response = externalApiClient.send(message);

        log.debug("Event processing completed. eventId={}, externalSuccess={}, processedAt={}",
                message.eventId(), response.success(), response.processedAt());
    }
}