package event.publisher;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = "event.publisher")
@EnableScheduling
public class EventPublisherApplication {
    public static void main(String[] args) {
        SpringApplication.run(EventPublisherApplication.class, args);
    }
}
