package messaging.api.messages;

public record MessageReceiveResponse(String clientMsgId, String status) { }
