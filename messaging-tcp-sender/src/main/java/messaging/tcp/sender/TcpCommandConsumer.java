package messaging.tcp.sender;

import messaging.common.messages.MessageTopics;
import messaging.common.messages.SecondarySendCommand;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;

@Component
@ConditionalOnProperty(prefix = "messaging.tcp.sender", name = "enabled", havingValue = "true")
public class TcpCommandConsumer {
    private final TcpSendService service;

    public TcpCommandConsumer(TcpSendService service) { this.service = service; }

    @KafkaListener(topics = MessageTopics.TCP_SEND, groupId = "messaging-tcp-sender")
    public void receive(ConsumerRecord<String, SecondarySendCommand> record)
            throws ExecutionException, InterruptedException {
        var command = record.value();
        if (command == null || !command.submission().clientMsgId().equals(record.key())) {
            throw new IllegalArgumentException("TCP command key differs from clientMsgId");
        }
        service.send(command);
    }
}
