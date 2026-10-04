package messaging.delivery.dispatch.receipt;

import messaging.common.delivery.DeliveryTopics;
import messaging.common.receipt.ReceiptEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ReceiptResultConsumer {
    private final ReceiptResultService service;
    public ReceiptResultConsumer(ReceiptResultService service) { this.service = service; }

    @KafkaListener(topics = "${dispatch.receipts.topic:" + DeliveryTopics.RECEIPT_RECEIVED + "}",
            groupId = "${dispatch.receipts.group:delivery-receipt-result-worker}", containerFactory = "receiptListenerContainerFactory")
    public void consume(ReceiptEvent receipt) { service.process(receipt); }
}
