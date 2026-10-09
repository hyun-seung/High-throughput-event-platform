package messaging.common.messages;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.protocol.ProtocolVersion;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class RedisConnectionRecoveryTest {
    @Test
    void tcpTimeoutPreservesExistingConnectionSettings() {
        var builder = ClientOptions.builder().autoReconnect(false)
                .socketOptions(io.lettuce.core.SocketOptions.builder()
                        .connectTimeout(Duration.ofSeconds(3)).tcpNoDelay(false).build());
        new RedisConnectionAutoConfiguration().messagingRedisSocketCustomizer(10).customize(builder);
        var configured = builder.build();
        assertFalse(configured.isAutoReconnect());
        assertEquals(Duration.ofSeconds(3), configured.getSocketOptions().getConnectTimeout());
        assertFalse(configured.getSocketOptions().isTcpNoDelay());
        assertTrue(configured.getSocketOptions().isEnableTcpUserTimeout());
        assertEquals(Duration.ofSeconds(10), configured.getSocketOptions().getTcpUserTimeout().getTcpUserTimeout());
    }

    @Test
    void invalidTcpTimeoutFailsBeforeCreatingConnections() {
        var config = new RedisConnectionAutoConfiguration();
        assertThrows(IllegalArgumentException.class, () -> config.messagingRedisSocketCustomizer(0));
        assertThrows(IllegalArgumentException.class, () -> config.messagingRedisSocketCustomizer(301));
    }

    @Test
    void commandTimeoutLeavesTheUnderlyingConnectionOpen() throws Exception {
        try (var server = new SilentRedis()) {
            var client = RedisClient.create(RedisURI.builder().withHost("127.0.0.1")
                    .withPort(server.listener.getLocalPort()).withTimeout(Duration.ofSeconds(3)).build());
            client.setOptions(ClientOptions.builder().protocolVersion(ProtocolVersion.RESP2).build());
            try (var connection = client.connect()) {
                assertEquals("PONG", connection.sync().ping());
                connection.setTimeout(Duration.ofMillis(200));
                server.silent.set(true);
                assertThrows(RedisCommandTimeoutException.class, () -> connection.sync().ping());
                boolean stillOpen = connection.isOpen();
                var socketOptions = client.getOptions().getSocketOptions();
                assertTrue(stillOpen, "A command timeout alone does not start reconnection");
                assertFalse(socketOptions.isEnableTcpUserTimeout());
            } finally {
                client.shutdown();
            }
        }
    }

    /** Respond to startup commands, then stop replying without closing the TCP socket. */
    private static final class SilentRedis implements AutoCloseable {
        final ServerSocket listener = new ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress());
        final AtomicBoolean silent = new AtomicBoolean();
        volatile Socket peer;
        volatile Throwable failure;
        final Thread worker;

        SilentRedis() throws Exception {
            worker = Thread.ofPlatform().daemon().start(() -> {
                try (Socket socket = listener.accept()) {
                    peer = socket;
                    var input = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                    String header;
                    while ((header = input.readLine()) != null) {
                        int count = Integer.parseInt(header.substring(1));
                        String command = null;
                        for (int index = 0; index < count; index++) {
                            input.readLine(); // All fixture commands have ASCII bulk-string arguments.
                            String argument = input.readLine();
                            if (index == 0) command = argument;
                        }
                        if (!silent.get()) {
                            String reply = "PING".equals(command) ? "+PONG\r\n" : "+OK\r\n";
                            socket.getOutputStream().write(reply.getBytes(StandardCharsets.UTF_8));
                            socket.getOutputStream().flush();
                        }
                    }
                } catch (Exception exception) {
                    if (!listener.isClosed()) failure = exception;
                }
            });
        }

        @Override public void close() throws Exception {
            listener.close();
            if (peer != null) peer.close();
            worker.join(2000);
            assertFalse(worker.isAlive());
            assertNull(failure);
        }
    }
}
