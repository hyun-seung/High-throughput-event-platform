package event.delivery.dispatch.receipt;

import event.common.delivery.DeliveryTopics;
import event.common.receipt.ReceiptEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ReceiptResultConsumer {
    private final ReceiptResultService service;
    public ReceiptResultConsumer(ReceiptResultService service) { this.service = service; }

    @KafkaListener(topics = "${dispatch.receipts.topic:" + DeliveryTopics.RECEIPT_RECEIVED + "}",
            groupId = "delivery-receipt-result-worker", containerFactory = "receiptListenerContainerFactory")
    public void consume(ReceiptEvent receipt) { service.process(receipt); }
}
