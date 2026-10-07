package messaging.carrier.sender;

import messaging.common.messages.HttpSendCommand;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.concurrent.ExecutionException;

@Component
public class CarrierHttpCommandConsumer {
    private final CarrierHttpSendService service;

    public CarrierHttpCommandConsumer(CarrierHttpSendService service) {
        this.service = Objects.requireNonNull(service);
    }

    @KafkaListener(topics = "${messaging.carrier.sender.topic}", groupId = "${messaging.carrier.sender.group-id}")
    public void receive(ConsumerRecord<String, HttpSendCommand> record) throws ExecutionException, InterruptedException {
        HttpSendCommand command = Objects.requireNonNull(record.value(), "Missing HTTP command");
        if (!command.request().clientMsgId().equals(record.key())) {
            throw new IllegalArgumentException("Carrier command key does not match clientMsgId");
        }
        service.send(command);
    }
}
