package event.receipt.security;

import event.receipt.config.ReceiptProperties;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

public final class ProviderAuthenticationFilter extends OncePerRequestFilter {
    private static final Pattern PATH = Pattern.compile("/api/v1/receipts/([A-Za-z0-9_-]{1,64})");
    private final ReceiptProperties properties;

    public ProviderAuthenticationFilter(ReceiptProperties properties) { this.properties = properties; }

    @Override
    protected boolean shouldNotFilterAsyncDispatch() {
        // Async HTTP completion resumes on another thread with a fresh security context.
        return false;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        var path = PATH.matcher(request.getServletPath());
        if ("POST".equals(request.getMethod()) && path.matches()) {
            String provider = path.group(1);
            String secret = properties.secret(provider);
            var headers = Collections.list(request.getHeaders("Authorization"));
            String header = headers.size() == 1 ? headers.getFirst() : "";
            if (secret.isEmpty() || header.length() > 263 || !header.startsWith("Bearer ")
                    || !MessageDigest.isEqual(secret.getBytes(StandardCharsets.UTF_8),
                    header.substring(7).getBytes(StandardCharsets.UTF_8))) {
                response.setStatus(401);
                return;
            }
            var context = SecurityContextHolder.createEmptyContext();
            context.setAuthentication(UsernamePasswordAuthenticationToken.authenticated(provider, null, List.of()));
            SecurityContextHolder.setContext(context);
        }
        chain.doFilter(request, response);
    }
}
