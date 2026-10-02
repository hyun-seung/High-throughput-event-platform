package event.publisher;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = "event.publisher")
@EnableScheduling
public class EventPublicationRecoveryApplication {
    public static void main(String[] args) {
        SpringApplication.run(EventPublicationRecoveryApplication.class, args);
    }
}
