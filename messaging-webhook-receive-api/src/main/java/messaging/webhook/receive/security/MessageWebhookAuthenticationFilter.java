package messaging.webhook.receive.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import messaging.common.messages.HttpCarrier;
import messaging.webhook.receive.config.MessageWebhookProperties;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/** Per-carrier bearer authentication before reading a possibly large webhook batch. */
public final class MessageWebhookAuthenticationFilter extends OncePerRequestFilter {
    private static final Pattern PATH = Pattern.compile("/api/v1/message-webhooks/(skt|kt|lgu)");
    private final MessageWebhookProperties properties;

    public MessageWebhookAuthenticationFilter(MessageWebhookProperties properties) { this.properties = properties; }

    @Override
    protected boolean shouldNotFilterAsyncDispatch() { return false; }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        var match = PATH.matcher(request.getServletPath());
        if ("POST".equals(request.getMethod()) && match.matches()) {
            HttpCarrier carrier = HttpCarrier.valueOf(match.group(1).toUpperCase(Locale.ROOT));
            String secret = properties.secret(carrier);
            var headers = Collections.list(request.getHeaders("Authorization"));
            String header = headers.size() == 1 ? headers.getFirst() : "";
            if (secret.isEmpty() || header.length() > 263 || !header.startsWith("Bearer ")
                    || !MessageDigest.isEqual(secret.getBytes(StandardCharsets.UTF_8),
                    header.substring(7).getBytes(StandardCharsets.UTF_8))) {
                response.setStatus(401);
                return;
            }
            var context = SecurityContextHolder.createEmptyContext();
            context.setAuthentication(UsernamePasswordAuthenticationToken.authenticated(carrier, null, List.of()));
            SecurityContextHolder.setContext(context);
        }
        chain.doFilter(request, response);
    }
}
