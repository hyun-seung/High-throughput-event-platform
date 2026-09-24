package event.receipt.config;

import event.receipt.security.ProviderAuthenticationFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Value;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.kafka.config.TopicBuilder;
import event.common.delivery.DeliveryTopics;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

import java.time.Clock;

@Configuration
public class ReceiptConfiguration {
    @Bean Clock receiptClock() { return Clock.systemUTC(); }

    @Bean
    NewTopic receiptTopic(@Value("${receipt.topic:" + DeliveryTopics.RECEIPT_RECEIVED + "}") String topic,
                          @Value("${receipt.partitions:3}") int partitions,
                          @Value("${receipt.replicas:1}") int replicas,
                          @Value("${receipt.min-insync-replicas:1}") int minIsr) {
        if (partitions < 1 || replicas < 1 || minIsr < 1 || minIsr > replicas) {
            throw new IllegalArgumentException("Invalid receipt topic replication settings");
        }
        return TopicBuilder.name(topic).partitions(partitions).replicas(replicas)
                .config("min.insync.replicas", Integer.toString(minIsr))
                .config("cleanup.policy", "delete").config("retention.ms", "604800000").build();
    }

    @Bean
    SecurityFilterChain receiptSecurity(HttpSecurity http, ReceiptProperties properties) throws Exception {
        return http.csrf(AbstractHttpConfigurer::disable)
                .formLogin(AbstractHttpConfigurer::disable).httpBasic(AbstractHttpConfigurer::disable)
                .requestCache(AbstractHttpConfigurer::disable)
                .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .exceptionHandling(e -> e.authenticationEntryPoint((request, response, error) -> response.setStatus(401)))
                .authorizeHttpRequests(a -> a
                        .dispatcherTypeMatchers(jakarta.servlet.DispatcherType.ERROR).permitAll()
                        .requestMatchers("/actuator/health", "/actuator/prometheus").permitAll()
                        .requestMatchers(org.springframework.http.HttpMethod.POST, "/api/v1/receipts/*").authenticated()
                        .anyRequest().denyAll())
                .addFilterBefore(new ProviderAuthenticationFilter(properties), UsernamePasswordAuthenticationFilter.class)
                .build();
    }
}
