package messaging.publisher;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = "messaging.publisher")
@EnableScheduling
public class MessagePublicationRecoveryApplication {
    public static void main(String[] args) {
        SpringApplication.run(MessagePublicationRecoveryApplication.class, args);
    }
}
