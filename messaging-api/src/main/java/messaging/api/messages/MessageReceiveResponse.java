package messaging.api.messages;

public record MessageReceiveResponse(String sendRequestId, String status) { }
