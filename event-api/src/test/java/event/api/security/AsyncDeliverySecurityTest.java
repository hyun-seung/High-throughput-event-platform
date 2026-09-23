package event.api.security;

import event.api.common.exception.GlobalExceptionHandler;
import event.api.delivery.controller.DeliveryController;
import event.api.delivery.dto.DeliveryResponse;
import event.api.delivery.service.DeliveryService;
import event.api.delivery.service.DeliveryAcceptanceException;
import event.api.requestcontrol.RequestLimiter;
import event.api.requestcontrol.filter.RequestControlFilter;
import event.api.requestcontrol.handler.RequestControlFailureHandler;
import event.api.requestcontrol.result.RequestLimitResult;
import event.api.requestcontrol.result.RequestLimitStatus;
import event.api.security.config.SecurityConfig;
import event.api.security.filter.JwtTokenAuthenticationProcessingFilter;
import event.api.security.handler.JwtAuthenticationEntryPoint;
import event.common.security.security.jwt.token.JwtHeaderTokenExtractor;
import event.common.security.security.jwt.token.JwtTokenVerifier;
import io.jsonwebtoken.Jwts;
import jakarta.servlet.Filter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.test.context.junit.jupiter.web.SpringJUnitWebConfig;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.request.async.AsyncRequestTimeoutException;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import tools.jackson.databind.json.JsonMapper;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringJUnitWebConfig(AsyncDeliverySecurityTest.Config.class)
class AsyncDeliverySecurityTest {
    @Autowired WebApplicationContext context;
    @Autowired DeliveryService service;
    @Autowired RequestLimiter limiter;
    @Autowired JwtTokenVerifier verifier;
    MockMvc mvc;

    @BeforeEach
    void setUp() {
        reset(service, limiter, verifier);
        when(verifier.parseClaims("valid-token"))
                .thenReturn(Jwts.claims().subject("1").add("username", "local-user").build());
        when(limiter.tryAcquire(1L))
                .thenReturn(new RequestLimitResult(RequestLimitStatus.ALLOWED, 99, 1, 100000));
        mvc = MockMvcBuilders.webAppContextSetup(context)
                .addFilters(context.getBean("springSecurityFilterChain", Filter.class)).build();
    }

    @Test
    void authenticatedAsyncResponseReturnsAcceptedAndConsumesQuotaOnlyOnce() throws Exception {
        var pending = new CompletableFuture<DeliveryResponse>();
        when(service.accept(any(), any(), any())).thenReturn(pending);
        var result = mvc.perform(post("/api/v1/deliveries").servletPath("/api/v1/deliveries")
                        .header("Authorization", "Bearer valid-token")
                        .header("Idempotency-Key", "async-test")
                        .contentType("application/json")
                        .content("{\"deliveryType\":\"EMAIL\",\"payload\":{}}"))
                .andExpect(request().asyncStarted()).andReturn();
        pending.complete(new DeliveryResponse("delivery-1", "ACCEPTED"));
        mvc.perform(asyncDispatch(result))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.data.deliveryId").value("delivery-1"));
        verify(limiter, times(1)).tryAcquire(1L);
        verify(service, times(1)).accept(any(), any(), any());
    }

    @Test
    void missingTokenCannotSubmitDelivery() throws Exception {
        mvc.perform(post("/api/v1/deliveries").servletPath("/api/v1/deliveries")
                        .header("Idempotency-Key", "unauthenticated-test")
                        .contentType("application/json")
                        .content("{\"deliveryType\":\"EMAIL\",\"payload\":{}}"))
                .andExpect(status().isUnauthorized());
        verifyNoInteractions(service, limiter);
    }

    @Test
    void failedPublicationReturns503WithSameKeyRetryGuidanceAndChargesQuotaOnce() throws Exception {
        assertUnconfirmedResponse(new DeliveryAcceptanceException(
                new IllegalStateException("internal broker information")));
    }

    @Test
    void mvcTimeoutAlsoReturnsUnconfirmedAcceptanceInsteadOf202() throws Exception {
        assertUnconfirmedResponse(new AsyncRequestTimeoutException());
    }

    private void assertUnconfirmedResponse(Exception failure) throws Exception {
        var pending = new CompletableFuture<DeliveryResponse>();
        when(service.accept(any(), any(), any())).thenReturn(pending);
        var result = mvc.perform(post("/api/v1/deliveries").servletPath("/api/v1/deliveries")
                        .header("Authorization", "Bearer valid-token")
                        .header("Idempotency-Key", "retry-test")
                        .contentType("application/json")
                        .content("{\"deliveryType\":\"EMAIL\",\"payload\":{}}"))
                .andExpect(request().asyncStarted()).andReturn();
        pending.completeExceptionally(failure);
        mvc.perform(asyncDispatch(result))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.data.code").value(9003))
                .andExpect(jsonPath("$.data.message").value(
                        "접수 여부를 확인하지 못했습니다. 동일한 Idempotency-Key와 요청 내용으로 재요청해 주세요."));
        verify(limiter, times(1)).tryAcquire(1L);
    }

    @Configuration
    @EnableWebMvc
    @EnableWebSecurity
    @Import({SecurityConfig.class, JwtTokenAuthenticationProcessingFilter.class,
            RequestControlFilter.class, RequestControlFailureHandler.class,
            JwtAuthenticationEntryPoint.class, DeliveryController.class, GlobalExceptionHandler.class})
    static class Config {
        @Bean JsonMapper jsonMapper() { return JsonMapper.builder().build(); }
        @Bean JwtHeaderTokenExtractor tokenExtractor() { return new JwtHeaderTokenExtractor(); }
        @Bean JwtTokenVerifier verifier() { return mock(JwtTokenVerifier.class); }
        @Bean RequestLimiter limiter() { return mock(RequestLimiter.class); }
        @Bean DeliveryService service() { return mock(DeliveryService.class); }
    }
}
