package messaging.tcp.sender;

import messaging.common.messages.SecondarySendCommand;
import messaging.common.messages.TcpSendResult;
import messaging.common.tcp.MessagingTcpRequest;
import messaging.common.tcp.MessagingTcpResponse;
import messaging.common.tcp.TcpFrames;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Clock;

/** Provisional length-prefixed JSON adapter for the second provider. */
@Component
public class TcpProviderClient {
    private final TcpSenderProperties properties;
    private final JsonMapper mapper;
    private final Clock clock;

    public TcpProviderClient(TcpSenderProperties properties, JsonMapper mapper, Clock clock) {
        this.properties = properties;
        this.mapper = mapper;
        this.clock = clock;
    }

    public TcpSendResult send(SecondarySendCommand command) {
        var submission = command.submission();
        var request = new MessagingTcpRequest(submission.clientMsgId(), submission.clientId(),
                submission.messageId(), submission.recipientNumber(),
                submission.messageCategory().name(), submission.secondarySendPayload(), clock.instant());
        byte[] body = mapper.writeValueAsBytes(request);
        if (body.length > TcpFrames.MAX_BYTES) return result(command, TcpSendResult.Status.INVALID_RESPONSE, 40005, null);
        try (var socket = new Socket()) {
            socket.connect(new InetSocketAddress(properties.host(), properties.port()),
                    Math.toIntExact(properties.connectTimeout().toMillis()));
            socket.setSoTimeout(Math.toIntExact(properties.responseTimeout().toMillis()));
            TcpFrames.write(socket.getOutputStream(), body);
            var reply = mapper.readValue(TcpFrames.read(socket.getInputStream()), MessagingTcpResponse.class);
            if (reply == null || !submission.clientMsgId().equals(reply.clientMsgId())
                    || reply.status() == null || reply.processedAt() == null) {
                return result(command, TcpSendResult.Status.INVALID_RESPONSE, 40005, null);
            }
            if ("success".equals(reply.status()) && reply.error() == null) {
                return result(command, TcpSendResult.Status.SUCCESS, null, null);
            }
            if ("fail".equals(reply.status()) && reply.error() != null
                    && reply.error().code() != null && reply.error().code() >= 70000
                    && reply.error().code() <= 79999 && reply.error().message() != null
                    && !reply.error().message().isBlank()) {
                int code = reply.error().code();
                return result(command, TcpSendResult.Status.FAILED, code, Integer.toString(code));
            }
            return result(command, TcpSendResult.Status.INVALID_RESPONSE, 40005, null);
        } catch (TcpFrames.InvalidFrameException malformed) {
            return result(command, TcpSendResult.Status.INVALID_RESPONSE, 40005, null);
        } catch (IOException failure) {
            return result(command, TcpSendResult.Status.TIMEOUT, 40004, null);
        } catch (RuntimeException malformed) {
            return result(command, TcpSendResult.Status.INVALID_RESPONSE, 40005, null);
        }
    }

    public TcpSendResult timeout(SecondarySendCommand command) {
        return result(command, TcpSendResult.Status.TIMEOUT, 40004, null);
    }

    private TcpSendResult result(SecondarySendCommand command, TcpSendResult.Status status,
                                 Integer errorCode, String providerCode) {
        String id = command.submission().clientMsgId();
        return new TcpSendResult(TcpSendResult.id(id, command.attemptId()), id, command.attemptId(),
                "TCP_RESPONSE", status, errorCode, providerCode, clock.instant());
    }
}
