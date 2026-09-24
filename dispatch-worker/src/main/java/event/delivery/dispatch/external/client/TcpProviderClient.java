package event.delivery.dispatch.external.client;

import event.common.delivery.DeliveryEvent;
import event.common.tcp.TcpDeliveryRequest;
import event.common.tcp.TcpDeliveryResponse;
import event.common.tcp.TcpFrames;
import event.delivery.dispatch.external.config.TcpProviderProperties;
import event.delivery.dispatch.external.dto.ProviderDispatchResponse;
import event.delivery.dispatch.port.SecondaryProviderClient;
import jakarta.annotation.PreDestroy;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static event.delivery.dispatch.external.client.ProviderFailureException.Kind.*;

@Component
@EnableConfigurationProperties(TcpProviderProperties.class)
public class TcpProviderClient implements SecondaryProviderClient, AutoCloseable {
    private final TcpProviderProperties properties;
    private final JsonMapper mapper;
    private final ScheduledExecutorService timeouts = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon().name("tcp-client-deadline").factory());

    public TcpProviderClient(TcpProviderProperties properties, JsonMapper mapper) {
        this.properties = properties;
        this.mapper = mapper;
    }

    @Override
    public ProviderDispatchResponse send(DeliveryEvent event, String idempotencyKey) {
        try (var socket = new Socket()) {
            byte[] body = mapper.writeValueAsBytes(TcpDeliveryRequest.from(event, idempotencyKey));
            if (body.length > TcpFrames.MAX_BYTES) throw new ProviderFailureException(PERMANENT_REJECTION);
            // SO_TIMEOUT covers reads only. Closing the socket also bounds blocked writes and slow trickles.
            var timeout = timeouts.schedule(() -> closeSocket(socket), properties.exchangeTimeout().toMillis(), TimeUnit.MILLISECONDS);
            try {
                socket.connect(new InetSocketAddress(properties.host(), properties.port()), (int) properties.connectTimeout().toMillis());
                socket.setSoTimeout((int) properties.exchangeTimeout().toMillis());
                TcpFrames.write(socket.getOutputStream(), body);
                var response = mapper.readValue(TcpFrames.read(socket.getInputStream()), TcpDeliveryResponse.class);
                if (response == null || !event.deliveryId().equals(response.deliveryId()) || !idempotencyKey.equals(response.attemptId())
                        || response.accepted() == null || response.processedAt() == null || response.code() == null) {
                    throw new ProviderFailureException(INVALID_RESPONSE);
                }
                if (response.accepted()) {
                    if (!"RECEIVED".equals(response.code())) throw new ProviderFailureException(INVALID_RESPONSE);
                    return new ProviderDispatchResponse(response.deliveryId(), true, response.processedAt(), "RECEIVED");
                }
                throw new ProviderFailureException(switch (response.code()) {
                    case "RETRY_1S" -> RETRY_1S;
                    case "RETRY_10S" -> RETRY_10S;
                    case "REJECTED" -> PERMANENT_REJECTION;
                    default -> INVALID_RESPONSE;
                });
            } finally {
                timeout.cancel(false);
            }
        } catch (TcpFrames.InvalidFrameException | JacksonException failure) {
            throw new ProviderFailureException(INVALID_RESPONSE);
        } catch (IOException failure) {
            throw new ProviderFailureException(NO_RESPONSE);
        }
    }

    private static void closeSocket(Socket socket) {
        try { socket.close(); } catch (IOException ignored) { }
    }

    @Override
    @PreDestroy
    public void close() { timeouts.shutdownNow(); }
}
