package messaging.webhook.receive;

import messaging.common.messages.MessageTopics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

import java.time.Clock;

@Configuration
public class MessageWebhookConfiguration {
    @Bean Clock webhookClock() { return Clock.systemUTC(); }

    @Bean
    NewTopic messageResultTopic(@Value("${message.webhook.partitions:3}") int partitions,
                                @Value("${message.webhook.replicas:1}") int replicas,
                                @Value("${message.webhook.min-insync-replicas:1}") int minIsr) {
        if (partitions < 1 || replicas < 1 || minIsr < 1 || minIsr > replicas) {
            throw new IllegalArgumentException("Invalid message result topic replication settings");
        }
        return TopicBuilder.name(MessageTopics.MSG_RESULT).partitions(partitions).replicas(replicas)
                .config("min.insync.replicas", Integer.toString(minIsr))
                .config("cleanup.policy", "delete").config("retention.ms", "604800000").build();
    }

    @Bean
    SecurityFilterChain webhookSecurity(HttpSecurity http, MessageWebhookProperties properties) throws Exception {
        return http.csrf(AbstractHttpConfigurer::disable)
                .formLogin(AbstractHttpConfigurer::disable).httpBasic(AbstractHttpConfigurer::disable)
                .requestCache(AbstractHttpConfigurer::disable)
                .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .exceptionHandling(e -> e.authenticationEntryPoint((request, response, error) -> response.setStatus(401)))
                .authorizeHttpRequests(a -> a
                        .dispatcherTypeMatchers(jakarta.servlet.DispatcherType.ERROR).permitAll()
                        .requestMatchers("/actuator/health", "/actuator/prometheus", "/livez", "/readyz").permitAll()
                        .requestMatchers(org.springframework.http.HttpMethod.POST, "/api/v1/message-webhooks/*").authenticated()
                        .anyRequest().denyAll())
                .addFilterBefore(new MessageWebhookAuthenticationFilter(properties),
                        UsernamePasswordAuthenticationFilter.class)
                .build();
    }
}
