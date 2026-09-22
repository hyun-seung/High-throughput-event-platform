package event.delivery.dispatch.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

@Configuration
@EnableConfigurationProperties(DispatchProperties.class)
public class DispatchConfiguration {

    @Bean
    public Clock dispatchClock() {
        return Clock.systemUTC();
    }
}
