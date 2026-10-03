package event.reference.cache;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import tools.jackson.databind.json.JsonMapper;

@Configuration
public class ReferenceCdcConfiguration {
    @Bean
    JsonMapper referenceCdcJsonMapper() { return JsonMapper.builder().build(); }

    @Bean
    DefaultErrorHandler referenceCdcErrorHandler() {
        // A Redis failure must not acknowledge and discard a source-table change.
        return new DefaultErrorHandler(new FixedBackOff(1000, FixedBackOff.UNLIMITED_ATTEMPTS));
    }
}
