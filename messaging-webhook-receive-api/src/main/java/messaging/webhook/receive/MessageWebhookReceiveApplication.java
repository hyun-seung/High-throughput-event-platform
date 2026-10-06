package messaging.webhook.receive;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class MessageWebhookReceiveApplication {
    public static void main(String[] args) {
        SpringApplication.run(MessageWebhookReceiveApplication.class, args);
    }
}
