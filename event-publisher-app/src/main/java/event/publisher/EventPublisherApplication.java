package event.publisher;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "event.publisher")
public class EventPublisherApplication {
    public static void main(String[] args) {
        SpringApplication.run(EventPublisherApplication.class, args);
    }
}
