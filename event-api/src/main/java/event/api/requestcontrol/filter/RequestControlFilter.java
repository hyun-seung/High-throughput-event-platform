package event.api.requestcontrol.filter;

import event.api.requestcontrol.RequestLimiter;
import event.api.requestcontrol.handler.RequestControlFailureHandler;
import event.api.requestcontrol.result.RequestLimitResult;
import event.api.requestcontrol.result.RequestLimitStatus;
import event.api.security.principal.AuthenticatedUser;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.servlet.FilterChain;
import jakarta.annotation.PostConstruct;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpMethod;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Objects;
import java.util.Locale;

@Component
@RequiredArgsConstructor
public class RequestControlFilter extends OncePerRequestFilter {

    private static final String DELIVERY_API_PATH = "/api/v1/deliveries";

    private final RequestLimiter requestLimiter;
    private final RequestControlFailureHandler failureHandler;
    private final MeterRegistry meterRegistry;

    @PostConstruct
    void initializeAdmissionMetrics() {
        // Publish a zero baseline before the first outcome so increase() can observe it.
        for (RequestLimitStatus status : RequestLimitStatus.values()) {
            meterRegistry.timer("delivery.admission.duration", "outcome",
                    status.name().toLowerCase(Locale.ROOT));
        }
        meterRegistry.timer("delivery.admission.duration", "outcome", "error");
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !HttpMethod.POST.matches(request.getMethod()) || !DELIVERY_API_PATH.equals(request.getServletPath());
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain
    ) throws ServletException, IOException {

        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

        if (Objects.isNull(authentication) || !authentication.isAuthenticated()
                || !(authentication.getPrincipal() instanceof AuthenticatedUser user)) {
            filterChain.doFilter(request, response);
            return;
        }

        Timer.Sample sample = Timer.start(meterRegistry);
        RequestLimitResult result;
        try {
            result = requestLimiter.tryAcquire(user.userId());
        } catch (RuntimeException failure) {
            sample.stop(meterRegistry.timer("delivery.admission.duration", "outcome", "error"));
            throw failure;
        }
        sample.stop(meterRegistry.timer("delivery.admission.duration", "outcome",
                result.status().name().toLowerCase(Locale.ROOT)));

        if (result.isAllowed()) {
            filterChain.doFilter(request, response);
            return;
        }

        failureHandler.handle(request, response, result);
    }
}
