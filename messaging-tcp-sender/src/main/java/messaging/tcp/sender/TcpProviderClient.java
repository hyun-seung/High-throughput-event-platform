package messaging.tcp.sender;

import messaging.common.messages.SecondarySendCommand;
import messaging.common.messages.TcpSendResult;
import messaging.common.tcp.TcpDeliveryRequest;
import messaging.common.tcp.TcpDeliveryResponse;
import messaging.common.tcp.TcpFrames;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Clock;

/** Temporary length-prefixed JSON adapter until the second provider's wire specification is analyzed. */
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
        var request = new TcpDeliveryRequest(submission.clientMsgId(), command.attemptId(),
                submission.clientId(), submission.messageCategory().name(),
                submission.secondarySendPayload(), submission.receivedAt(), 1);
        byte[] body = mapper.writeValueAsBytes(request);
        if (body.length > TcpFrames.MAX_BYTES) return result(command, TcpSendResult.Status.INVALID_RESPONSE, 40005, null);
        try (var socket = new Socket()) {
            socket.connect(new InetSocketAddress(properties.host(), properties.port()),
                    Math.toIntExact(properties.connectTimeout().toMillis()));
            socket.setSoTimeout(Math.toIntExact(properties.responseTimeout().toMillis()));
            TcpFrames.write(socket.getOutputStream(), body);
            var reply = mapper.readValue(TcpFrames.read(socket.getInputStream()), TcpDeliveryResponse.class);
            if (reply == null || !submission.clientMsgId().equals(reply.deliveryId())
                    || !command.attemptId().equals(reply.attemptId()) || reply.accepted() == null
                    || reply.code() == null || reply.code().length() > 64 || reply.processedAt() == null) {
                return result(command, TcpSendResult.Status.INVALID_RESPONSE, 40005, null);
            }
            if (reply.accepted()) {
                if (!"RECEIVED".equals(reply.code())) {
                    return result(command, TcpSendResult.Status.INVALID_RESPONSE, 40005, reply.code());
                }
                return result(command, TcpSendResult.Status.SUCCESS, null, reply.code());
            }
            return result(command, TcpSendResult.Status.FAILED, providerFailureCode(reply.code()), reply.code());
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

    private static int providerFailureCode(String code) {
        if (code.length() == 5 && code.chars().allMatch(character -> character >= '0' && character <= '9')) {
            int numericCode = Integer.parseInt(code);
            if (numericCode >= 70000 && numericCode <= 79999) return numericCode;
        }
        // The local simulator still returns text codes until the provider wire contract is available.
        return 70001;
    }

    private TcpSendResult result(SecondarySendCommand command, TcpSendResult.Status status,
                                 Integer errorCode, String providerCode) {
        String id = command.submission().clientMsgId();
        return new TcpSendResult(TcpSendResult.id(id, command.attemptId()), id, command.attemptId(),
                "TCP_RESPONSE", status, errorCode, providerCode, clock.instant());
    }
}
