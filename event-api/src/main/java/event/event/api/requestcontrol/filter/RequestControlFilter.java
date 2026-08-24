package event.event.api.requestcontrol.filter;

import event.event.api.requestcontrol.RequestLimiter;
import event.event.api.requestcontrol.handler.RequestControlFailureHandler;
import event.event.api.requestcontrol.result.RequestLimitResult;
import event.event.api.security.principal.AuthenticatedUser;
import jakarta.servlet.FilterChain;
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

@Component
@RequiredArgsConstructor
public class RequestControlFilter extends OncePerRequestFilter {

    private static final String EVENT_API_PATH = "/api/v1/events";

    private final RequestLimiter requestLimiter;
    private final RequestControlFailureHandler failureHandler;

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !HttpMethod.POST.matches(request.getMethod()) || !EVENT_API_PATH.equals(request.getServletPath());
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

        RequestLimitResult result = requestLimiter.tryAcquire(user.userId());

        if (result.isAllowed()) {
            filterChain.doFilter(request, response);
            return;
        }

        failureHandler.handle(request, response, result);
    }
}