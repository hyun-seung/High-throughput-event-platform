package messaging.complete;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Map;

@Configuration
public class CompleteManagerConfiguration {
    @Bean DefaultErrorHandler completeErrorHandler() {
        // A failed SQL transaction rolls back the entire poll. Never skip a failed batch or
        // report a BatchListenerFailedException that would commit an unpersisted prefix.
        var handler = new DefaultErrorHandler(new FixedBackOff(1000L, FixedBackOff.UNLIMITED_ATTEMPTS));
        handler.setClassifications(Map.of(Exception.class, true), true);
        return handler;
    }
}
