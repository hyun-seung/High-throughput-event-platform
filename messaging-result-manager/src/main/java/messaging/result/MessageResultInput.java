package messaging.result;

import messaging.common.messages.CarrierHttpResult;
import messaging.common.messages.MessageWebhookBatch;
import messaging.common.messages.PreSendFailure;
import messaging.common.messages.TcpSendResult;

/** Typed inputs to MSG-RESULT-MANAGER after source and Kafka key validation. */
public sealed interface MessageResultInput permits MessageResultInput.Webhook,
        MessageResultInput.Http, MessageResultInput.PreSend, MessageResultInput.Tcp {
    record Webhook(MessageWebhookBatch batch) implements MessageResultInput { }
    record Http(CarrierHttpResult result) implements MessageResultInput { }
    record PreSend(PreSendFailure failure) implements MessageResultInput { }
    record Tcp(TcpSendResult result) implements MessageResultInput { }
}
