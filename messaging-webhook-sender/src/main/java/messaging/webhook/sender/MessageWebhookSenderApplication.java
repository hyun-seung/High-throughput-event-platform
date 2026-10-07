package messaging.webhook.sender;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication(scanBasePackages = "messaging.webhook.sender")
public class MessageWebhookSenderApplication {
    public static void main(String[] args) {
        SpringApplication.run(MessageWebhookSenderApplication.class, args);
    }
}
