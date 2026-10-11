package messaging.api.messages;

import messaging.api.security.AuthenticatedUser;
import org.junit.jupiter.api.Test;
import org.springframework.core.MethodParameter;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;
import org.springframework.web.bind.support.WebDataBinderFactory;

import java.time.Clock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class MessageReceiveControllerTest {
    private final MessageUsageLimiter usage = mock(MessageUsageLimiter.class);
    private final MessageDuplicateGuard duplicates = mock(MessageDuplicateGuard.class);
    private final MessageOriginStore origins = mock(MessageOriginStore.class);
    private final MessageRequestPublisher publisher = mock(MessageRequestPublisher.class);
    private static final String REQUEST = """
            {"messageId":"customer-1","recipientNumber":"01012345678",
             "messageCategory":"GENERAL","payload":{"message":"hello"}}
            """;

    private MockMvc mvc(long timeoutMs) {
        when(duplicates.claim(eq(42L), any(), anyString()))
                .thenReturn(new MessageDuplicateGuard.Claim("stable-id", "key", "value", true, true));
        var service = new MessageReceiveService(usage, duplicates, origins, publisher, Clock.systemUTC(), timeoutMs);
        return MockMvcBuilders.standaloneSetup(new MessageReceiveController(service))
                .setControllerAdvice(new GlobalExceptionHandler())
                .setCustomArgumentResolvers(new HandlerMethodArgumentResolver() {
                    public boolean supportsParameter(MethodParameter parameter) {
                        return parameter.getParameterType() == AuthenticatedUser.class;
                    }

                    public Object resolveArgument(MethodParameter parameter, ModelAndViewContainer container,
                                                  NativeWebRequest request, WebDataBinderFactory factory) {
                        return new AuthenticatedUser(42L, "test-user");
                    }
                }).build();
    }

    @Test
    void http200IsReturnedOnlyAfterKafkaAck() throws Exception {
        var send = new CompletableFuture<>();
        var published = new CountDownLatch(1);
        when(publisher.publish(any())).thenAnswer(call -> {
            published.countDown();
            return send;
        });
        var mvc = mvc(5000);
        try (var executor = Executors.newSingleThreadExecutor()) {
            var response = executor.submit(() -> mvc.perform(post("/api/v1/messages")
                    .contentType(MediaType.APPLICATION_JSON).content(REQUEST)));
            try {
                assertTrue(published.await(2, TimeUnit.SECONDS));
                assertThrows(TimeoutException.class, () -> response.get(100, TimeUnit.MILLISECONDS));
            } finally {
                send.complete(null);
            }
            response.get(2, TimeUnit.SECONDS).andExpect(status().isOk())
                    .andExpect(jsonPath("$.status").value(200))
                    .andExpect(jsonPath("$.data.clientMsgId").value("stable-id"))
                    .andExpect(jsonPath("$.data.status").value("RECEIVED"));
        }
    }

    @Test
    void failedKafkaAckReturnsHttp503() throws Exception {
        when(publisher.publish(any())).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")));
        mvc(5000).perform(post("/api/v1/messages").contentType(MediaType.APPLICATION_JSON).content(REQUEST))
                .andExpect(status().isServiceUnavailable()).andExpect(jsonPath("$.status").value(503));
        verify(duplicates, never()).release(any());
    }

    @Test
    void unconfirmedKafkaAckReturnsHttp503AfterTimeout() throws Exception {
        when(publisher.publish(any())).thenReturn(new CompletableFuture<>());
        mvc(10).perform(post("/api/v1/messages").contentType(MediaType.APPLICATION_JSON).content(REQUEST))
                .andExpect(status().isServiceUnavailable()).andExpect(jsonPath("$.status").value(503));
        verify(duplicates, never()).release(any());
    }
}
